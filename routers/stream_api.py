import time
from collections import deque
from threading import Lock
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
import pandas as pd
from routers.analytics_api import record_execution

router = APIRouter(prefix="/api/stream", tags=["Streaming"])


class PatientStreamPayload(BaseModel):
    id: Optional[int] = None
    diagnosis: str = "M"
    radius_mean: float = 20.0
    texture_mean: Optional[float] = 15.0
    perimeter_mean: Optional[float] = 120.0
    area_mean: Optional[float] = 1000.0
    smoothness_mean: Optional[float] = 0.1
    extra_features: Optional[Dict[str, Any]] = None


class PathologyStreamPayload(BaseModel):
    text: str
    sample_name: Optional[str] = "Live Stream Sample"
    medical_specialty: Optional[str] = "Pathology"


def initialise_streaming(app):
    """Menginisialisasi buffer dan statistik streaming di app.state."""
    app.state.stream_lock = Lock()
    app.state.stream_history = deque(maxlen=50)
    app.state.stream_stats = {
        "total_patients": 0,
        "total_pathology": 0,
        "last_stream_time": None,
        "latest_ingest_time_ms": None,
        "mode_stats": {
            "hadoop": {"count": 0, "total_ms": 0.0, "avg_ms": None, "last_ms": None},
            "spark": {"count": 0, "total_ms": 0.0, "avg_ms": None, "last_ms": None}
        }
    }
    app.state.streamed_text_data = []


@router.post("/patient")
def ingest_patient_stream(payload: PatientStreamPayload, request: Request):
    """Menerima aliran data pasien real-time dan mengukur waktu proses injeksi ke engine."""
    try:
        start_time = time.time()
        mode = getattr(request.app.state, "execution_mode", "spark")
        now_str = time.strftime("%H:%M:%S")

        patient_dict = payload.model_dump()
        extra = patient_dict.pop("extra_features", None) or {}
        patient_data = {**patient_dict, **extra}

        if patient_data.get("id") is None:
            patient_data["id"] = int(time.time() * 1000) % 10000000

        with request.app.state.stream_lock:
            if mode == "hadoop":
                df = getattr(request.app.state, "hadoop_patients_df", None)
                if df is None:
                    raise HTTPException(
                        status_code=500, detail="Data pasien Hadoop belum diinisialisasi.")
                new_row_df = pd.DataFrame([patient_data])
                request.app.state.hadoop_patients_df = pd.concat(
                    [df, new_row_df], ignore_index=True)

            else:  # mode spark
                spark = getattr(request.app.state, "spark", None)
                if spark is None:
                    raise HTTPException(
                        status_code=500, detail="Spark Session belum diinisialisasi.")

                curr_spark_df = getattr(
                    request.app.state, "patients_spark_df", None)
                new_row_df = pd.DataFrame([patient_data])
                new_spark_df = spark.createDataFrame(new_row_df)

                if curr_spark_df is not None:
                    updated_spark_df = curr_spark_df.unionByName(
                        new_spark_df, allowMissingColumns=True)
                else:
                    updated_spark_df = new_spark_df

                request.app.state.patients_spark_df = updated_spark_df
                updated_spark_df.createOrReplaceTempView("patients")

            # Hitung waktu proses tepat setelah selesai disuntikkan
            exec_time_sec = round(time.time() - start_time, 4)
            exec_time_ms = round(exec_time_sec * 1000, 2)

            # Catat statistik dan analitik
            record_execution(request, "stream_ingest", exec_time_sec)

            m_stat = request.app.state.stream_stats["mode_stats"][mode]
            m_stat["count"] += 1
            m_stat["total_ms"] += exec_time_ms
            m_stat["last_ms"] = exec_time_ms
            m_stat["avg_ms"] = round(m_stat["total_ms"] / m_stat["count"], 2)

            request.app.state.stream_stats["total_patients"] += 1
            request.app.state.stream_stats["last_stream_time"] = now_str
            request.app.state.stream_stats["latest_ingest_time_ms"] = exec_time_ms

            history_item = {
                "type": "patient",
                "id": patient_data["id"],
                "diagnosis": patient_data["diagnosis"],
                "radius_mean": patient_data["radius_mean"],
                "timestamp": now_str,
                "mode": mode,
                "ingest_time_ms": exec_time_ms
            }
            request.app.state.stream_history.appendleft(history_item)

        return {
            "status": "success",
            "message": f"Data pasien #{patient_data['id']} disuntikkan ke {mode.upper()}",
            "mode": mode,
            "patient": history_item,
            "ingest_time_ms": exec_time_ms,
            "ingest_time_sec": exec_time_sec
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pathology")
def ingest_pathology_stream(payload: PathologyStreamPayload, request: Request):
    """Menerima aliran data teks laporan patologi real-time dan mengukur waktu proses injeksi ke engine."""
    try:
        start_time = time.time()
        mode = getattr(request.app.state, "execution_mode", "spark")
        now_str = time.strftime("%H:%M:%S")

        raw_line = f"{payload.sample_name}, {payload.medical_specialty}, {payload.text}\n"

        with request.app.state.stream_lock:
            if mode == "hadoop":
                text_data = getattr(request.app.state, "hadoop_text_data", None)
                if text_data is None:
                    request.app.state.hadoop_text_data = []
                request.app.state.hadoop_text_data.insert(0, raw_line)
            else:  # mode spark
                if not hasattr(request.app.state, "streamed_text_data"):
                    request.app.state.streamed_text_data = []
                request.app.state.streamed_text_data.insert(0, raw_line)

            exec_time_sec = round(time.time() - start_time, 4)
            exec_time_ms = round(exec_time_sec * 1000, 2)

            record_execution(request, "stream_ingest", exec_time_sec)

            m_stat = request.app.state.stream_stats["mode_stats"][mode]
            m_stat["count"] += 1
            m_stat["total_ms"] += exec_time_ms
            m_stat["last_ms"] = exec_time_ms
            m_stat["avg_ms"] = round(m_stat["total_ms"] / m_stat["count"], 2)

            request.app.state.stream_stats["total_pathology"] += 1
            request.app.state.stream_stats["last_stream_time"] = now_str
            request.app.state.stream_stats["latest_ingest_time_ms"] = exec_time_ms

            history_item = {
                "type": "pathology",
                "summary": (payload.text[:80] + "...") if len(payload.text) > 80 else payload.text,
                "timestamp": now_str,
                "mode": mode,
                "ingest_time_ms": exec_time_ms
            }
            request.app.state.stream_history.appendleft(history_item)

        return {
            "status": "success",
            "message": f"Data teks patologi disuntikkan ke {mode.upper()}",
            "mode": mode,
            "pathology": history_item,
            "ingest_time_ms": exec_time_ms,
            "ingest_time_sec": exec_time_sec
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
def get_stream_status(request: Request):
    """Mengembalikan statistik streaming, waktu proses per engine, dan rekaman live data terbaru."""
    with request.app.state.stream_lock:
        stats = dict(request.app.state.stream_stats)
        history = list(request.app.state.stream_history)

    mode_stats = stats.get("mode_stats", {})
    h_avg = mode_stats.get("hadoop", {}).get("avg_ms")
    s_avg = mode_stats.get("spark", {}).get("avg_ms")

    comparison_note = "Belum cukup data kedua mode untuk dibandingkan."
    if h_avg is not None and s_avg is not None and h_avg > 0 and s_avg > 0:
        if h_avg < s_avg:
            diff_ratio = round(s_avg / h_avg, 2)
            comparison_note = f"Hadoop {diff_ratio}x lebih cepat daripada Spark dalam menerima & menginjeksi data real-time."
        else:
            diff_ratio = round(h_avg / s_avg, 2)
            comparison_note = f"Spark {diff_ratio}x lebih cepat daripada Hadoop dalam menerima & menginjeksi data real-time."

    return {
        "active_mode": getattr(request.app.state, "execution_mode", "spark"),
        "stats": stats,
        "recent_streams": history[:15],
        "comparison_note": comparison_note
    }
