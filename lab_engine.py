"""
Clinical Laboratory Data Analytics - engine pemrosesan data2/
==============================================================
Alur data (sama dengan pola folder data/ pada proyek ini):

    data2/ (landing zone)  ->  load / ingest  ->  clean + standardise  ->  query  ->  API/dashboard

Dua engine, dipilih oleh EXEC_MODE di main.py:
    - SparkLabStore  : PySpark DataFrame + Spark SQL (EXEC_MODE=spark)
    - PandasLabStore : HDFS client + pandas             (EXEC_MODE=hadoop)

Kedua engine memakai aturan pembersihan yang SAMA (konstanta di bawah),
sehingga hasil analitik dan waktu eksekusinya bisa dibandingkan secara adil.

Sumber data:
    - patient_encounters.csv        : CSV terstruktur (dimensi, dimuat sekali saat startup)
    - lab_results_batch_N.json      : JSON Lines bersarang (fakta, datang per batch)
"""

import glob
import json
import math
import os
import re
import time
from collections import deque
from datetime import date, datetime
from decimal import Decimal
from threading import Lock
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Konfigurasi lokasi data
# --------------------------------------------------------------------------
DATA2_DIR = os.getenv("DATA2_DIR", "data2")
HDFS_DATA2_DIR = os.getenv("HDFS_DATA2_DIR", "/data2")
ENCOUNTER_FILENAMES = ("patient_encounters.csv", "patient_encounters - Copy.csv")
BATCH_GLOB = "lab_results_batch_*.json"

# --------------------------------------------------------------------------
# Aturan standarisasi (satu sumber kebenaran untuk kedua engine)
# --------------------------------------------------------------------------
# Satuan baku + rentang referensi per jenis tes (low, high); None = tidak ada batas.
TEST_STANDARDS = {
    "Blood Glucose": {"unit": "mg/dL",   "ref": (70.0, 140.0)},
    "Creatinine":    {"unit": "mg/dL",   "ref": (0.6, 1.3)},
    "Hemoglobin":    {"unit": "g/dL",    "ref": (12.0, 17.5)},
    "WBC Count":     {"unit": "10^3/uL", "ref": (4.5, 11.0)},
    "Lipid Panel":   {"unit": "mg/dL",   "ref": (None, 200.0)},
}
# (test_name, satuan_asal) -> faktor pengali menuju satuan baku
UNIT_FACTORS = {
    ("Blood Glucose", "mmol/L"): 18.016,      # mmol/L -> mg/dL
    ("Creatinine", "umol/L"): 1.0 / 88.4,     # umol/L -> mg/dL
}
SENTINEL_VALUE = -999.0                        # kode error instrumen pada result_value

QUALITY_FLAGS = (
    "OK", "PENDING_RESULT", "READ_ERROR", "CALIBRATION_ERROR",
    "UNPARSEABLE_RESULT", "UNKNOWN_UNIT", "ORPHAN_ENCOUNTER",
    "TIMESTAMP_BEFORE_ADMISSION",
)


# --------------------------------------------------------------------------
# Utilitas
# --------------------------------------------------------------------------
def _sanitize(obj: Any) -> Any:
    """Ubah NaN/Inf/Timestamp/Decimal/numpy menjadi tipe yang aman untuk JSON."""
    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, pd.Timestamp)):
        return None if pd.isna(obj) else obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, date):
        return obj.isoformat()
    if obj is pd.NaT:
        return None
    return obj


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _batch_number(path: str) -> int:
    m = re.search(r"batch_(\d+)", os.path.basename(path))
    return int(m.group(1)) if m else 0


def _local_encounter_path() -> Optional[str]:
    for name in ENCOUNTER_FILENAMES:
        p = os.path.join(DATA2_DIR, name)
        if os.path.exists(p):
            return p
    return None


def _local_batch_files() -> List[str]:
    return sorted(glob.glob(os.path.join(DATA2_DIR, BATCH_GLOB)), key=_batch_number)


def _pct(part: float, whole: float) -> Optional[float]:
    return round(part * 100.0 / whole, 2) if whole else None


# --------------------------------------------------------------------------
# Fragmen SQL yang dibangkitkan dari konstanta (bukan dari input pengguna)
# --------------------------------------------------------------------------
def _sql_unit_known() -> str:
    parts = [f"(test_name = '{t}' AND unit_raw = '{s['unit']}')" for t, s in TEST_STANDARDS.items()]
    parts += [f"(test_name = '{t}' AND unit_raw = '{u}')" for (t, u) in UNIT_FACTORS]
    return "(" + " OR ".join(parts) + ")"


def _sql_unit_std() -> str:
    whens = " ".join(f"WHEN test_name = '{t}' THEN '{s['unit']}'" for t, s in TEST_STANDARDS.items())
    return f"CASE {whens} ELSE NULL END"


def _sql_result_std() -> str:
    whens = " ".join(
        f"WHEN test_name = '{t}' AND unit_raw = '{u}' THEN result_num * {f}"
        for (t, u), f in UNIT_FACTORS.items()
    )
    return f"CASE {whens} ELSE result_num END"


def _sql_ref(idx: int) -> str:
    whens = " ".join(
        f"WHEN test_name = '{t}' THEN {s['ref'][idx] if s['ref'][idx] is not None else 'NULL'}"
        for t, s in TEST_STANDARDS.items()
    )
    return f"CASE {whens} ELSE NULL END"


# ==========================================================================
# Basis: logika bersama (logging batch, ringkasan, bentuk response)
# ==========================================================================
class BaseLabStore:
    engine_name = "base"

    def __init__(self):
        self.lock = Lock()
        self.batch_log: deque = deque(maxlen=100)
        self.last_arrival: Optional[float] = None
        self.enc_raw_rows = 0
        self.enc_dup_removed = 0
        self.raw_total = 0
        self.dup_removed = 0
        self.landing_source = "local"

    # ---- harus diimplementasikan oleh engine --------------------------------
    def load_encounters(self) -> Dict[str, Any]: raise NotImplementedError
    def _ingest_impl(self, kind: str, data: Any) -> Dict[str, Any]: raise NotImplementedError
    def _reset_labs(self) -> None: raise NotImplementedError
    def _enc_stats(self) -> Dict[str, Any]: raise NotImplementedError
    def _lab_stats(self) -> Dict[str, Any]: raise NotImplementedError
    def _flag_counts(self) -> Dict[str, int]: raise NotImplementedError
    def _unit_conversion_counts(self) -> List[Dict[str, Any]]: raise NotImplementedError
    def _valid_set_checks(self) -> Dict[str, int]: raise NotImplementedError
    def _simple_agg(self) -> Dict[str, Any]: raise NotImplementedError
    def _relational(self) -> Dict[str, Any]: raise NotImplementedError
    def _preview(self, limit: int, flag: Optional[str]) -> List[Dict[str, Any]]: raise NotImplementedError
    def landing_zone_files(self) -> List[Dict[str, Any]]: raise NotImplementedError

    # ---- ingest dengan pengukuran Velocity ----------------------------------
    def ingest(self, kind: str, data: Any, batch_id: Optional[int] = None,
               source: str = "producer", sent_at: Optional[float] = None) -> Dict[str, Any]:
        arrival = time.time()
        with self.lock:
            t0 = time.perf_counter()
            r = self._ingest_impl(kind, data)
            total_ms = (time.perf_counter() - t0) * 1000.0
            finished = time.time()

            gap = None if self.last_arrival is None else round(arrival - self.last_arrival, 3)
            self.last_arrival = arrival
            self.raw_total += r["raw_rows"]
            self.dup_removed += r["duplicates_removed"]

            entry = {
                "batch_id": batch_id if batch_id is not None else len(self.batch_log) + 1,
                "source": source,
                "engine": self.engine_name,
                "arrival_time": _iso(arrival),
                "inter_arrival_sec": gap,
                "records": r["raw_rows"],
                "duplicates_removed": r["duplicates_removed"],
                "clean_rows": r["clean_rows"],
                "event_time_min": r.get("event_min"),
                "event_time_max": r.get("event_max"),
                "parse_ms": round(r["parse_ms"], 2),
                "clean_ms": round(r["clean_ms"], 2),
                "merge_ms": round(r["merge_ms"], 2),
                "processing_latency_ms": round(total_ms, 2),
                "throughput_rec_per_sec": round(r["raw_rows"] / (total_ms / 1000.0), 1) if total_ms > 0 else None,
                "end_to_end_latency_ms": round((finished - sent_at) * 1000.0, 2) if sent_at else None,
            }
            self.batch_log.append(entry)
            return _sanitize(entry)

    def reset(self) -> None:
        with self.lock:
            self._reset_labs()
            self.batch_log.clear()
            self.last_arrival = None
            self.raw_total = 0
            self.dup_removed = 0

    def load_all(self) -> Dict[str, Any]:
        """Bulk load historis semua batch dari landing zone (demonstrasi Volume)."""
        files = self.landing_zone_files()
        t0 = time.perf_counter()
        entries = []
        for f in files:
            entries.append(self.ingest("file", f["batch"], batch_id=f["batch"], source="landing-zone"))
        return {
            "batches_loaded": len(entries),
            "total_seconds": round(time.perf_counter() - t0, 3),
            "batches": entries,
        }

    def batches(self) -> Dict[str, Any]:
        log = list(self.batch_log)
        gaps = [b["inter_arrival_sec"] for b in log if b["inter_arrival_sec"] is not None]
        lats = [b["processing_latency_ms"] for b in log]
        return _sanitize({
            "engine": self.engine_name,
            "batches_ingested": len(log),
            "avg_inter_arrival_sec": round(sum(gaps) / len(gaps), 3) if gaps else None,
            "avg_processing_latency_ms": round(sum(lats) / len(lats), 2) if lats else None,
            "max_processing_latency_ms": round(max(lats), 2) if lats else None,
            "log": log,
        })

    # ---- Volume --------------------------------------------------------------
    def overview(self) -> Dict[str, Any]:
        enc = self._enc_stats()
        lab = self._lab_stats()
        files = self.landing_zone_files()
        total_bytes = sum(f["size_bytes"] for f in files)
        lab_rows = lab["clean_rows"]
        matched = lab["rows_matching_encounter"]
        return _sanitize({
            "engine": self.engine_name,
            "landing_zone": {
                "source": self.landing_source,
                "lab_batch_files": len(files),
                "lab_total_size_mb": round(total_bytes / 1024 / 1024, 2),
                "files": files,
            },
            "encounters": {
                "raw_rows": self.enc_raw_rows,
                "duplicates_removed": self.enc_dup_removed,
                "clean_rows": enc["clean_rows"],
                "distinct_patients": enc["distinct_patients"],
                "admission_min": enc["admission_min"],
                "admission_max": enc["admission_max"],
            },
            "lab_results": {
                "raw_rows_ingested": self.raw_total,
                "duplicates_removed": self.dup_removed,
                "clean_rows": lab_rows,
                "valid_rows": lab["valid_rows"],
                "distinct_encounters_with_labs": lab["distinct_encounters"],
                "avg_valid_tests_per_encounter_with_labs": (
                    round(lab["valid_rows"] / lab["distinct_encounters"], 2) if lab["distinct_encounters"] else None),
            },
            "relationships": {
                "join_key": "lab_results.encounter_id -> encounters.encounter_id (many-to-one)",
                "lab_rows_matching_encounter": matched,
                "join_match_rate_pct": _pct(matched, lab_rows),
            },
        })

    # ---- Veracity ------------------------------------------------------------
    def quality(self) -> Dict[str, Any]:
        flags = self._flag_counts()
        total = sum(flags.values())
        checks = self._valid_set_checks()
        conv = self._unit_conversion_counts()
        conv_total = sum(c["rows"] for c in conv)
        valid = flags.get("OK", 0)

        def issue(name, dimension, before, after, handling):
            return {"issue": name, "dimension": dimension, "before": before, "after": after, "handling": handling}

        issues = [
            issue("Duplicate lab records (same lab_record_id)", "uniqueness",
                  self.dup_removed, checks["duplicate_lab_ids"],
                  "Drop duplicates by lab_record_id (keep first), also across batches"),
            issue("Duplicate encounters (same encounter_id)", "uniqueness",
                  self.enc_dup_removed, checks["duplicate_encounter_ids"],
                  "Drop duplicates by encounter_id (rows are identical)"),
            issue("Pending results (result_value = null)", "completeness",
                  flags.get("PENDING_RESULT", 0), checks["null_or_sentinel_in_valid"],
                  "Flag PENDING_RESULT; excluded from analytics"),
            issue("Instrument read errors (result_value = empty string)", "validity",
                  flags.get("READ_ERROR", 0), checks["null_or_sentinel_in_valid"],
                  "Flag READ_ERROR; excluded from analytics"),
            issue("Calibration errors (sentinel -999)", "validity",
                  flags.get("CALIBRATION_ERROR", 0), checks["null_or_sentinel_in_valid"],
                  "Flag CALIBRATION_ERROR; sentinel never treated as a measurement"),
            issue("Timestamp before admission", "consistency",
                  flags.get("TIMESTAMP_BEFORE_ADMISSION", 0), checks["ts_before_admission_in_valid"],
                  "Flag TIMESTAMP_BEFORE_ADMISSION; excluded from temporal analysis"),
            issue("Inconsistent units (mmol/L, umol/L)", "consistency",
                  conv_total, 0,
                  "Convert to standard unit: glucose mmol/L x 18.016 -> mg/dL; creatinine umol/L / 88.4 -> mg/dL"),
        ]
        for extra, label in (("UNKNOWN_UNIT", "Unknown unit for test"), ("ORPHAN_ENCOUNTER", "Lab without matching encounter"),
                             ("UNPARSEABLE_RESULT", "Non-numeric result")):
            if flags.get(extra, 0):
                issues.append(issue(label, "validity", flags[extra], 0, f"Flag {extra}; excluded"))
        return _sanitize({
            "engine": self.engine_name,
            "summary": {
                "raw_rows_ingested": self.raw_total,
                "after_deduplication": total,
                "valid_rows": valid,
                "excluded_rows": total - valid,
                "valid_pct_of_clean": _pct(valid, total),
                "retained_pct_of_raw": _pct(valid, self.raw_total),
            },
            "flag_distribution": [{"quality_flag": k, "rows": flags.get(k, 0), "pct": _pct(flags.get(k, 0), total)}
                                  for k in QUALITY_FLAGS if flags.get(k, 0)],
            "issues": issues,
            "unit_conversions": conv,
            "note": "'before' = problem count found in ingested data; 'after' = same problem measured inside the "
                    "valid (quality_flag = OK) set. Error-type counts are measured after de-duplication.",
        })

    # ---- Value (+ kompleksitas) ---------------------------------------------
    def simple_agg(self) -> Dict[str, Any]:
        t0 = time.perf_counter()
        res = self._simple_agg()
        res["execution_time_sec"] = round(time.perf_counter() - t0, 4)
        res.update({"operation": "A - Simple aggregation", "input_tables": 1, "joins": 0,
                    "operations": "GROUP BY + COUNT on encounters"})
        return _sanitize(res)

    def relational(self) -> Dict[str, Any]:
        t0 = time.perf_counter()
        res = self._relational()
        res["execution_time_sec"] = round(time.perf_counter() - t0, 4)
        res.update({"operation": "B - Relational and temporal analysis", "input_tables": 2, "joins": 4,
                    "operations": "JOIN lab_results x encounters, CASE flags, GROUP BY department/test/day/diagnosis, "
                                  "timestamp arithmetic"})
        return _sanitize(res)

    def compare(self, repeats: int = 3) -> Dict[str, Any]:
        repeats = max(1, min(repeats, 10))
        self.simple_agg(); self.relational()  # warm-up (JIT/cache) tidak dihitung
        a = [self.simple_agg()["execution_time_sec"] for _ in range(repeats)]
        b = [self.relational()["execution_time_sec"] for _ in range(repeats)]
        lab = self._lab_stats()
        enc = self._enc_stats()
        avg_a, avg_b = sum(a) / len(a), sum(b) / len(b)
        return _sanitize({
            "engine": self.engine_name,
            "repeats": repeats,
            "rows": {"encounters": enc["clean_rows"], "lab_results": lab["clean_rows"]},
            "operation_A": {"name": "Simple aggregation", "input_tables": 1, "joins": 0,
                            "avg_sec": round(avg_a, 4), "runs_sec": a},
            "operation_B": {"name": "Relational + temporal", "input_tables": 2, "joins": 4,
                            "avg_sec": round(avg_b, 4), "runs_sec": b},
            "slowdown_B_vs_A": round(avg_b / avg_a, 2) if avg_a > 0 else None,
        })

    def preview(self, limit: int = 100, flag: Optional[str] = None) -> Dict[str, Any]:
        rows = self._preview(limit, flag)
        return _sanitize({"engine": self.engine_name, "flag": flag, "count": len(rows), "data": rows})


# ==========================================================================
# Engine 1: Apache Spark (EXEC_MODE=spark)
# ==========================================================================
class SparkLabStore(BaseLabStore):
    engine_name = "spark"

    def __init__(self, spark):
        super().__init__()
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        self.spark = spark
        # Default 200 partisi shuffle sangat boros di local[1] (tiap shuffle = 200 task kecil).
        # Dataset ini hanya ~100 ribu baris, jadi cukup 8 partisi.
        spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        self.enc_df = None
        self.lab_df = None
        self._enc_schema = StructType([
            StructField("encounter_id", StringType()), StructField("patient_id", StringType()),
            StructField("age", IntegerType()), StructField("gender", StringType()),
            StructField("department", StringType()), StructField("admission_timestamp", StringType()),
            StructField("diagnosis_code", StringType()),
        ])
        # result_value bertipe campuran di sumber (float / "" / null / -999) -> dibaca sebagai string,
        # lalu di-cast aman pada tahap pembersihan.
        self._lab_schema = StructType([
            StructField("lab_record_id", StringType()), StructField("encounter_id", StringType()),
            StructField("test_batch_timestamp", StringType()),
            StructField("test_details", StructType([
                StructField("test_name", StringType()), StructField("result_value", StringType()),
                StructField("unit", StringType()), StructField("reference_range", StringType())])),
            StructField("instrument_metadata", StructType([
                StructField("analyzer_id", StringType()), StructField("status", StringType())])),
        ])

    # -- startup ---------------------------------------------------------------
    def load_encounters(self):
        path = _local_encounter_path()
        if not path:
            raise FileNotFoundError(f"patient_encounters.csv tidak ditemukan di {DATA2_DIR}/")
        raw = self.spark.read.csv(os.path.abspath(path), header=True, schema=self._enc_schema)
        self.enc_raw_rows = raw.count()
        clean = (raw.dropDuplicates(["encounter_id"])
                 .selectExpr("encounter_id", "patient_id", "age", "gender", "department", "diagnosis_code",
                             "try_cast(admission_timestamp AS TIMESTAMP) AS admission_ts")
                 .cache())
        n = clean.count()
        self.enc_dup_removed = self.enc_raw_rows - n
        self.enc_df = clean
        clean.createOrReplaceTempView("encounters")
        return {"encounters_raw": self.enc_raw_rows, "encounters_clean": n, "duplicates_removed": self.enc_dup_removed}

    # -- ingest ------------------------------------------------------------------
    def _flatten(self, raw):
        return raw.selectExpr(
            "lab_record_id", "encounter_id",
            "try_cast(test_batch_timestamp AS TIMESTAMP) AS test_ts",
            "test_details.test_name AS test_name", "test_details.result_value AS result_raw",
            "test_details.unit AS unit_raw", "instrument_metadata.analyzer_id AS analyzer_id",
            "instrument_metadata.status AS status")

    def _ingest_impl(self, kind, data):
        spark = self.spark
        if self.enc_df is None:
            raise RuntimeError("Encounters belum dimuat.")
        # 1) PARSE (JSON bersarang -> DataFrame datar)
        t = time.perf_counter()
        if kind == "file":
            path = next((f["path"] for f in self.landing_zone_files() if f["batch"] == data), None)
            if not path:
                raise FileNotFoundError(f"Batch {data} tidak ada di landing zone.")
            raw = spark.read.schema(self._lab_schema).json(os.path.abspath(path))
        else:
            rdd = spark.sparkContext.parallelize([json.dumps(r) for r in data])
            raw = spark.read.schema(self._lab_schema).json(rdd)
        flat = self._flatten(raw).cache()
        raw_rows = flat.count()
        parse_ms = (time.perf_counter() - t) * 1000

        # 2) CLEAN: dedup (dalam batch + lintas batch), flag kualitas, standarisasi satuan
        t = time.perf_counter()
        dedup = flat.dropDuplicates(["lab_record_id"])
        if self.lab_df is not None:
            dedup = dedup.join(self.lab_df.select("lab_record_id"), "lab_record_id", "left_anti")
        dedup.createOrReplaceTempView("lab_new")
        # localCheckpoint (eager) MEMUTUS lineage. Tanpa ini, plan batch ke-n memuat lab_df batch
        # sebelumnya dua kali (union + anti-join) sehingga plan tumbuh eksponensial -> latensi naik & OOM.
        clean = spark.sql(self._clean_sql()).localCheckpoint()
        clean_rows = clean.count()
        ev = clean.agg({"test_ts": "min"}).first()[0], clean.agg({"test_ts": "max"}).first()[0]
        clean_ms = (time.perf_counter() - t) * 1000

        # 3) MERGE ke tabel utama + view Spark SQL
        t = time.perf_counter()
        self.lab_df = clean if self.lab_df is None else self.lab_df.unionByName(clean)
        self.lab_df.createOrReplaceTempView("lab_results")
        merge_ms = (time.perf_counter() - t) * 1000
        flat.unpersist()
        return {"raw_rows": raw_rows, "duplicates_removed": raw_rows - clean_rows, "clean_rows": clean_rows,
                "parse_ms": parse_ms, "clean_ms": clean_ms, "merge_ms": merge_ms,
                "event_min": ev[0], "event_max": ev[1]}

    def _clean_sql(self) -> str:
        return f"""
        WITH base AS (
            SELECT n.lab_record_id, n.encounter_id, n.test_ts, n.test_name, n.result_raw, n.unit_raw,
                   n.analyzer_id, n.status, e.admission_ts,
                   try_cast(n.result_raw AS DOUBLE) AS result_num
            FROM lab_new n LEFT JOIN encounters e ON n.encounter_id = e.encounter_id
        ),
        flagged AS (
            SELECT *, CASE
                WHEN status = 'PENDING_RESULT' OR result_raw IS NULL THEN 'PENDING_RESULT'
                WHEN status = 'READ_ERROR' OR trim(result_raw) = '' THEN 'READ_ERROR'
                WHEN status = 'CALIBRATION_ERROR' OR result_num = {SENTINEL_VALUE} THEN 'CALIBRATION_ERROR'
                WHEN result_num IS NULL THEN 'UNPARSEABLE_RESULT'
                WHEN NOT {_sql_unit_known()} THEN 'UNKNOWN_UNIT'
                WHEN admission_ts IS NULL THEN 'ORPHAN_ENCOUNTER'
                WHEN test_ts IS NULL OR test_ts < admission_ts THEN 'TIMESTAMP_BEFORE_ADMISSION'
                ELSE 'OK' END AS quality_flag
            FROM base
        ),
        std AS (
            SELECT *, {_sql_unit_std()} AS unit_std,
                   CASE WHEN quality_flag = 'OK' THEN {_sql_result_std()} END AS result_std,
                   {_sql_ref(0)} AS ref_low, {_sql_ref(1)} AS ref_high
            FROM flagged
        )
        SELECT lab_record_id, encounter_id, test_ts, admission_ts, test_name, result_raw, unit_raw,
               unit_std, result_std, analyzer_id, status, quality_flag,
               CASE WHEN quality_flag = 'OK' THEN
                    CASE WHEN (ref_low IS NOT NULL AND result_std < ref_low)
                           OR (ref_high IS NOT NULL AND result_std > ref_high) THEN 1 ELSE 0 END
               END AS abnormal
        FROM std
        """

    def _reset_labs(self):
        if self.lab_df is not None:
            self.lab_df.unpersist()
        self.lab_df = None
        self.spark.catalog.dropTempView("lab_results")

    # -- statistik -----------------------------------------------------------------
    def _rows(self, sql: str) -> List[Dict[str, Any]]:
        return [r.asDict() for r in self.spark.sql(sql).collect()]

    def _enc_stats(self):
        r = self._rows("""SELECT COUNT(*) AS clean_rows, COUNT(DISTINCT patient_id) AS distinct_patients,
                          MIN(admission_ts) AS admission_min, MAX(admission_ts) AS admission_max FROM encounters""")[0]
        return r

    def _lab_stats(self):
        if self.lab_df is None:
            return {"clean_rows": 0, "valid_rows": 0, "distinct_encounters": 0, "rows_matching_encounter": 0}
        return self._rows("""SELECT COUNT(*) AS clean_rows,
                  SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END) AS valid_rows,
                  COUNT(DISTINCT encounter_id) AS distinct_encounters,
                  SUM(CASE WHEN admission_ts IS NOT NULL THEN 1 ELSE 0 END) AS rows_matching_encounter
                  FROM lab_results""")[0]

    def _flag_counts(self):
        if self.lab_df is None:
            return {}
        return {r["quality_flag"]: r["n"] for r in self._rows(
            "SELECT quality_flag, COUNT(*) AS n FROM lab_results GROUP BY quality_flag")}

    def _unit_conversion_counts(self):
        if self.lab_df is None:
            return []
        out = []
        for (t, u), f in UNIT_FACTORS.items():
            n = self._rows(f"SELECT COUNT(*) AS n FROM lab_results WHERE test_name = '{t}' AND unit_raw = '{u}'")[0]["n"]
            out.append({"test_name": t, "from_unit": u, "to_unit": TEST_STANDARDS[t]["unit"], "factor": round(f, 6), "rows": n})
        return out

    def _valid_set_checks(self):
        enc_dups = self._rows("SELECT COUNT(*) - COUNT(DISTINCT encounter_id) AS d FROM encounters")[0]["d"]
        if self.lab_df is None:
            return {"duplicate_lab_ids": 0, "duplicate_encounter_ids": enc_dups,
                    "null_or_sentinel_in_valid": 0, "ts_before_admission_in_valid": 0}
        r = self._rows(f"""SELECT COUNT(*) - COUNT(DISTINCT lab_record_id) AS dup_lab,
                 SUM(CASE WHEN quality_flag = 'OK' AND (result_std IS NULL OR result_std = {SENTINEL_VALUE}) THEN 1 ELSE 0 END) AS bad_val,
                 SUM(CASE WHEN quality_flag = 'OK' AND test_ts < admission_ts THEN 1 ELSE 0 END) AS bad_ts
                 FROM lab_results""")[0]
        return {"duplicate_lab_ids": r["dup_lab"], "duplicate_encounter_ids": enc_dups,
                "null_or_sentinel_in_valid": r["bad_val"] or 0, "ts_before_admission_in_valid": r["bad_ts"] or 0}

    # -- analitik -------------------------------------------------------------------
    def _simple_agg(self):
        rows = self._rows("""SELECT department, COUNT(*) AS encounters FROM encounters
                             GROUP BY department ORDER BY encounters DESC""")
        total = sum(r["encounters"] for r in rows)
        for r in rows:
            r["pct"] = _pct(r["encounters"], total)
        return {"question": "How many encounters does each department handle?", "encounters_by_department": rows}

    def _relational(self):
        if self.lab_df is None:
            raise RuntimeError("Belum ada data lab. Jalankan load-all atau stream batch dulu.")
        base = "FROM lab_results l JOIN encounters e ON l.encounter_id = e.encounter_id WHERE l.quality_flag = 'OK'"
        dept = self._rows(f"""
            WITH enc_cnt AS (SELECT department, COUNT(*) AS encounters_total FROM encounters GROUP BY department)
            SELECT e.department, c.encounters_total, COUNT(*) AS lab_tests,
                   ROUND(COUNT(*) / c.encounters_total, 2) AS tests_per_encounter,
                   ROUND(AVG(l.abnormal) * 100, 2) AS abnormal_pct,
                   ROUND(AVG((unix_timestamp(l.test_ts) - unix_timestamp(e.admission_ts)) / 3600.0), 2) AS avg_hours_to_result
            {base.replace('WHERE', 'JOIN enc_cnt c ON c.department = e.department WHERE')}
            GROUP BY e.department, c.encounters_total ORDER BY lab_tests DESC""")
        matrix = self._rows(f"""SELECT e.department, l.test_name, COUNT(*) AS tests, ROUND(AVG(l.abnormal) * 100, 2) AS abnormal_pct
                                {base} GROUP BY e.department, l.test_name ORDER BY e.department, tests DESC""")
        daily = self._rows(f"""SELECT CAST(DATE(l.test_ts) AS STRING) AS day, COUNT(*) AS tests, SUM(l.abnormal) AS abnormal_tests
                               {base} GROUP BY DATE(l.test_ts) ORDER BY day""")
        diag = self._rows(f"""SELECT e.diagnosis_code, l.test_name, COUNT(*) AS tests, ROUND(AVG(l.abnormal) * 100, 2) AS abnormal_pct
                              {base} GROUP BY e.diagnosis_code, l.test_name ORDER BY e.diagnosis_code, l.test_name""")
        return _relational_payload(dept, matrix, daily, diag)

    def _preview(self, limit, flag):
        if self.lab_df is None:
            return []
        where = ""
        if flag:
            if flag not in QUALITY_FLAGS:
                raise ValueError(f"flag harus salah satu dari {QUALITY_FLAGS}")
            where = f"WHERE quality_flag = '{flag}'"
        return self._rows(f"""SELECT lab_record_id, encounter_id, test_ts, test_name, result_raw, unit_raw, unit_std,
                              ROUND(result_std, 3) AS result_std, analyzer_id, status, quality_flag, abnormal
                              FROM lab_results {where} LIMIT {int(limit)}""")

    def landing_zone_files(self):
        self.landing_source = "local"
        return [{"batch": _batch_number(p), "name": os.path.basename(p), "path": p, "size_bytes": os.path.getsize(p)}
                for p in _local_batch_files()]


def _relational_payload(dept, matrix, daily, diag) -> Dict[str, Any]:
    top = dept[0] if dept else None
    hi = max(dept, key=lambda d: d["abnormal_pct"]) if dept else None
    return {
        "question": "Which departments drive laboratory utilisation, and how many results fall outside reference ranges?",
        "department_utilisation": dept,
        "department_x_test": matrix,
        "daily_utilisation": daily,
        "abnormal_by_diagnosis_test": diag,
        "headline": ({"highest_volume_department": top["department"], "highest_volume_tests": top["lab_tests"],
                      "highest_abnormal_department": hi["department"], "highest_abnormal_pct": hi["abnormal_pct"]}
                     if top else None),
        "limitations": ["Synthetic data: abnormal rates are not clinical prevalence.",
                        "Only quality_flag = OK results are analysed; excluded rows may bias volumes.",
                        "Reference ranges are a single adult range, not adjusted for age or sex."],
    }


# ==========================================================================
# Engine 2: HDFS client + pandas (EXEC_MODE=hadoop)
# ==========================================================================
class PandasLabStore(BaseLabStore):
    engine_name = "hadoop"

    def __init__(self, hdfs_client=None):
        super().__init__()
        self.hdfs = hdfs_client
        self.enc: Optional[pd.DataFrame] = None
        self.lab: Optional[pd.DataFrame] = None

    # -- baca dari HDFS, fallback ke lokal --------------------------------------
    def _hdfs_list(self):
        if self.hdfs is None:
            return None
        try:
            items = self.hdfs.list(HDFS_DATA2_DIR, status=True)
            return [(n, s["length"]) for n, s in items if re.match(r"lab_results_batch_\d+\.json$", n)]
        except Exception:
            return None

    def landing_zone_files(self):
        listed = self._hdfs_list()
        if listed:
            self.landing_source = "hdfs"
            files = [{"batch": _batch_number(n), "name": n, "path": f"{HDFS_DATA2_DIR}/{n}", "size_bytes": sz} for n, sz in listed]
        else:
            self.landing_source = "local"
            files = [{"batch": _batch_number(p), "name": os.path.basename(p), "path": p, "size_bytes": os.path.getsize(p)}
                     for p in _local_batch_files()]
        return sorted(files, key=lambda f: f["batch"])

    def _read_batch_lines(self, batch: int) -> List[Dict[str, Any]]:
        f = next((x for x in self.landing_zone_files() if x["batch"] == batch), None)
        if not f:
            raise FileNotFoundError(f"Batch {batch} tidak ada di landing zone.")
        if self.landing_source == "hdfs":
            with self.hdfs.read(f["path"], encoding="utf-8") as reader:
                text = reader.read()
        else:
            with open(f["path"], encoding="utf-8") as fh:
                text = fh.read()
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def load_encounters(self):
        df = None
        if self.hdfs is not None:
            try:
                for name in ENCOUNTER_FILENAMES:
                    try:
                        with self.hdfs.read(f"{HDFS_DATA2_DIR}/{name}", encoding="utf-8") as reader:
                            df = pd.read_csv(reader)
                        self.landing_source = "hdfs"
                        break
                    except Exception:
                        continue
            except Exception:
                df = None
        if df is None:
            path = _local_encounter_path()
            if not path:
                raise FileNotFoundError(f"patient_encounters.csv tidak ditemukan di HDFS {HDFS_DATA2_DIR}/ maupun {DATA2_DIR}/")
            df = pd.read_csv(path)
            self.landing_source = "local"
        self.enc_raw_rows = len(df)
        df = df.drop_duplicates("encounter_id").copy()
        df["admission_ts"] = pd.to_datetime(df["admission_timestamp"], errors="coerce")
        self.enc = df.drop(columns=["admission_timestamp"]).reset_index(drop=True)
        self.enc_dup_removed = self.enc_raw_rows - len(self.enc)
        return {"encounters_raw": self.enc_raw_rows, "encounters_clean": len(self.enc), "duplicates_removed": self.enc_dup_removed}

    # -- ingest -------------------------------------------------------------------
    def _ingest_impl(self, kind, data):
        if self.enc is None:
            raise RuntimeError("Encounters belum dimuat.")
        t = time.perf_counter()
        records = self._read_batch_lines(data) if kind == "file" else data
        flat = pd.json_normalize(records)
        flat = flat.rename(columns={
            "test_details.test_name": "test_name", "test_details.result_value": "result_raw",
            "test_details.unit": "unit_raw", "instrument_metadata.analyzer_id": "analyzer_id",
            "instrument_metadata.status": "status", "test_batch_timestamp": "test_ts"})
        flat = flat[["lab_record_id", "encounter_id", "test_ts", "test_name", "result_raw", "unit_raw", "analyzer_id", "status"]]
        raw_rows = len(flat)
        parse_ms = (time.perf_counter() - t) * 1000

        t = time.perf_counter()
        flat = flat.drop_duplicates("lab_record_id")
        if self.lab is not None:
            flat = flat[~flat["lab_record_id"].isin(self.lab["lab_record_id"])]
        clean = self._clean(flat)
        clean_ms = (time.perf_counter() - t) * 1000

        t = time.perf_counter()
        self.lab = clean if self.lab is None else pd.concat([self.lab, clean], ignore_index=True)
        merge_ms = (time.perf_counter() - t) * 1000
        return {"raw_rows": raw_rows, "duplicates_removed": raw_rows - len(clean), "clean_rows": len(clean),
                "parse_ms": parse_ms, "clean_ms": clean_ms, "merge_ms": merge_ms,
                "event_min": clean["test_ts"].min(), "event_max": clean["test_ts"].max()}

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.reset_index(drop=True)  # wajib: index harus sejajar dengan hasil merge di bawah
        df["test_ts"] = pd.to_datetime(df["test_ts"], errors="coerce")
        rv = df["result_raw"]
        is_null = rv.isna()
        is_blank = rv.map(lambda v: isinstance(v, str) and v.strip() == "")
        df["result_raw"] = rv.map(lambda v: None if v is None or (isinstance(v, float) and math.isnan(v)) else str(v))
        result_num = pd.to_numeric(rv, errors="coerce")
        df = df.merge(self.enc[["encounter_id", "admission_ts"]], on="encounter_id", how="left")

        known = pd.Series(False, index=df.index)
        for t, s in TEST_STANDARDS.items():
            known |= (df["test_name"] == t) & (df["unit_raw"] == s["unit"])
        for (t, u) in UNIT_FACTORS:
            known |= (df["test_name"] == t) & (df["unit_raw"] == u)

        conds = [
            (df["status"] == "PENDING_RESULT") | is_null,
            (df["status"] == "READ_ERROR") | is_blank,
            (df["status"] == "CALIBRATION_ERROR") | (result_num == SENTINEL_VALUE),
            result_num.isna(),
            ~known,
            df["admission_ts"].isna(),
            df["test_ts"].isna() | (df["test_ts"] < df["admission_ts"]),
        ]
        names = ["PENDING_RESULT", "READ_ERROR", "CALIBRATION_ERROR", "UNPARSEABLE_RESULT", "UNKNOWN_UNIT",
                 "ORPHAN_ENCOUNTER", "TIMESTAMP_BEFORE_ADMISSION"]
        df["quality_flag"] = np.select(conds, names, default="OK")

        factor = pd.Series(1.0, index=df.index)
        for (t, u), f in UNIT_FACTORS.items():
            factor = factor.where(~((df["test_name"] == t) & (df["unit_raw"] == u)), f)
        ok = df["quality_flag"] == "OK"
        df["unit_std"] = df["test_name"].map({t: s["unit"] for t, s in TEST_STANDARDS.items()})
        df["result_std"] = (result_num * factor).where(ok)
        low = df["test_name"].map({t: s["ref"][0] for t, s in TEST_STANDARDS.items()}).astype(float)
        high = df["test_name"].map({t: s["ref"][1] for t, s in TEST_STANDARDS.items()}).astype(float)
        abn = ((df["result_std"] < low) | (df["result_std"] > high)).astype(float)
        df["abnormal"] = abn.where(ok)
        return df[["lab_record_id", "encounter_id", "test_ts", "admission_ts", "test_name", "result_raw", "unit_raw",
                   "unit_std", "result_std", "analyzer_id", "status", "quality_flag", "abnormal"]]

    def _reset_labs(self):
        self.lab = None

    # -- statistik ----------------------------------------------------------------
    def _enc_stats(self):
        e = self.enc
        return {"clean_rows": len(e), "distinct_patients": int(e["patient_id"].nunique()),
                "admission_min": e["admission_ts"].min(), "admission_max": e["admission_ts"].max()}

    def _lab_stats(self):
        l = self.lab
        if l is None:
            return {"clean_rows": 0, "valid_rows": 0, "distinct_encounters": 0, "rows_matching_encounter": 0}
        return {"clean_rows": len(l), "valid_rows": int((l["quality_flag"] == "OK").sum()),
                "distinct_encounters": int(l["encounter_id"].nunique()),
                "rows_matching_encounter": int(l["admission_ts"].notna().sum())}

    def _flag_counts(self):
        return {} if self.lab is None else {k: int(v) for k, v in self.lab["quality_flag"].value_counts().items()}

    def _unit_conversion_counts(self):
        if self.lab is None:
            return []
        return [{"test_name": t, "from_unit": u, "to_unit": TEST_STANDARDS[t]["unit"], "factor": round(f, 6),
                 "rows": int(((self.lab["test_name"] == t) & (self.lab["unit_raw"] == u)).sum())}
                for (t, u), f in UNIT_FACTORS.items()]

    def _valid_set_checks(self):
        enc_dups = int(len(self.enc) - self.enc["encounter_id"].nunique())
        if self.lab is None:
            return {"duplicate_lab_ids": 0, "duplicate_encounter_ids": enc_dups,
                    "null_or_sentinel_in_valid": 0, "ts_before_admission_in_valid": 0}
        l = self.lab
        ok = l["quality_flag"] == "OK"
        return {"duplicate_lab_ids": int(len(l) - l["lab_record_id"].nunique()), "duplicate_encounter_ids": enc_dups,
                "null_or_sentinel_in_valid": int((ok & (l["result_std"].isna() | (l["result_std"] == SENTINEL_VALUE))).sum()),
                "ts_before_admission_in_valid": int((ok & (l["test_ts"] < l["admission_ts"])).sum())}

    # -- analitik -----------------------------------------------------------------
    def _simple_agg(self):
        g = self.enc.groupby("department").size().sort_values(ascending=False)
        total = int(g.sum())
        rows = [{"department": d, "encounters": int(n), "pct": _pct(n, total)} for d, n in g.items()]
        return {"question": "How many encounters does each department handle?", "encounters_by_department": rows}

    def _relational(self):
        if self.lab is None:
            raise RuntimeError("Belum ada data lab. Jalankan load-all atau stream batch dulu.")
        v = self.lab[self.lab["quality_flag"] == "OK"].merge(
            self.enc[["encounter_id", "department", "diagnosis_code"]], on="encounter_id", how="inner")
        v["hours"] = (v["test_ts"] - v["admission_ts"]).dt.total_seconds() / 3600.0
        enc_cnt = self.enc.groupby("department").size().rename("encounters_total")

        d = v.groupby("department").agg(lab_tests=("lab_record_id", "size"), abnormal_pct=("abnormal", "mean"),
                                        avg_hours_to_result=("hours", "mean")).join(enc_cnt).reset_index()
        d["tests_per_encounter"] = (d["lab_tests"] / d["encounters_total"]).round(2)
        d["abnormal_pct"] = (d["abnormal_pct"] * 100).round(2)
        d["avg_hours_to_result"] = d["avg_hours_to_result"].round(2)
        d = d.sort_values("lab_tests", ascending=False)[
            ["department", "encounters_total", "lab_tests", "tests_per_encounter", "abnormal_pct", "avg_hours_to_result"]]

        m = (v.groupby(["department", "test_name"]).agg(tests=("lab_record_id", "size"), abnormal_pct=("abnormal", "mean"))
             .reset_index())
        m["abnormal_pct"] = (m["abnormal_pct"] * 100).round(2)
        m = m.sort_values(["department", "tests"], ascending=[True, False])

        day = (v.assign(day=v["test_ts"].dt.strftime("%Y-%m-%d")).groupby("day")
               .agg(tests=("lab_record_id", "size"), abnormal_tests=("abnormal", "sum")).reset_index().sort_values("day"))
        day["abnormal_tests"] = day["abnormal_tests"].astype(int)

        dg = (v.groupby(["diagnosis_code", "test_name"]).agg(tests=("lab_record_id", "size"), abnormal_pct=("abnormal", "mean"))
              .reset_index())
        dg["abnormal_pct"] = (dg["abnormal_pct"] * 100).round(2)
        dg = dg.sort_values(["diagnosis_code", "test_name"])
        return _relational_payload(d.to_dict("records"), m.to_dict("records"), day.to_dict("records"), dg.to_dict("records"))

    def _preview(self, limit, flag):
        if self.lab is None:
            return []
        l = self.lab
        if flag:
            if flag not in QUALITY_FLAGS:
                raise ValueError(f"flag harus salah satu dari {QUALITY_FLAGS}")
            l = l[l["quality_flag"] == flag]
        cols = ["lab_record_id", "encounter_id", "test_ts", "test_name", "result_raw", "unit_raw", "unit_std",
                "result_std", "analyzer_id", "status", "quality_flag", "abnormal"]
        out = l[cols].head(int(limit)).copy()
        out["result_std"] = out["result_std"].round(3)
        return out.to_dict("records")
