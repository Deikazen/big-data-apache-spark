"""
Simulasi Pengiriman Data Real-Time (Data Stream Producer)
---------------------------------------------------------
Script ini membaca dataset CSV lokal (breast_cancer_data.csv & mtsamples.csv)
dan mensimulasikan aliran data (streaming) terus-menerus ke server FastAPI.
Data yang dikirim akan langsung ditangkap oleh server baik pada mode Hadoop maupun mode Spark,
serta mengukur dan membandingkan waktu proses ingest data antara kedua engine tersebut.

Cara menjalankan:
    python stream_producer.py
    python stream_producer.py --interval 1.0 --type both
    python stream_producer.py --type patients --interval 2.0 --count 50
"""

import os
import sys
import time
import random
import argparse
import requests
import pandas as pd

DEFAULT_TARGET_URL = "http://localhost:8000"
CANCER_CSV_PATH = "data/breast_cancer_data.csv"
MTSAMPLES_CSV_PATH = "data/mtsamples.csv"


def check_server_status(base_url: str):
    """Mengecek apakah server FastAPI sedang aktif dan mendeteksi mode yang berjalan."""
    try:
        res = requests.get(f"{base_url}/api/mode", timeout=3.0)
        if res.status_code == 200:
            data = res.json()
            return True, data.get("mode", "unknown"), data.get("engine", "")
    except requests.exceptions.RequestException:
        pass
    return False, None, None


def load_datasets():
    """Memuat data referensi dari file CSV untuk bahan streaming."""
    patients_pool = []
    pathology_pool = []

    # 1. Dataset Pasien Kanker
    if os.path.exists(CANCER_CSV_PATH):
        df = pd.read_csv(CANCER_CSV_PATH)
        cols = [c for c in df.columns if not str(c).startswith("Unnamed")]
        patients_pool = df[cols].to_dict(orient="records")
        print(f"[DATA] Memuat {len(patients_pool)} baris referensi pasien dari {CANCER_CSV_PATH}")
    else:
        print(f"[WARN] File {CANCER_CSV_PATH} tidak ditemukan! Menggunakan data sintetis.")

    # 2. Dataset Teks Laporan Patologi
    if os.path.exists(MTSAMPLES_CSV_PATH):
        df_text = pd.read_csv(MTSAMPLES_CSV_PATH)
        for _, row in df_text.iterrows():
            text = str(row.get("transcription") or row.get("description") or "").strip()
            if text and text != "nan" and len(text) > 20:
                pathology_pool.append({
                    "sample_name": str(row.get("sample_name", "Clinical Report")),
                    "medical_specialty": str(row.get("medical_specialty", "Pathology")),
                    "text": text[:350]
                })
        print(f"[DATA] Memuat {len(pathology_pool)} baris teks referensi dari {MTSAMPLES_CSV_PATH}")
    else:
        print(f"[WARN] File {MTSAMPLES_CSV_PATH} tidak ditemukan! Menggunakan teks sintetis.")

    return patients_pool, pathology_pool


def run_stream(target_url: str, interval: float, stream_type: str, max_count: int):
    print("\n" + "=" * 75)
    print("      📡 SIMULATOR DATA STREAMING REAL-TIME (BENCHMARK DURASI) 📡")
    print("=" * 75)
    print(f"Target URL : {target_url}")
    print(f"Interval   : {interval} detik per data")
    print(f"Tipe Aliran: {stream_type.upper()}")
    print(f"Jumlah Max : {'Tanpa Batas (Loop terus)' if max_count <= 0 else max_count}")
    print("-" * 75)

    is_alive, active_mode, engine_desc = check_server_status(target_url)
    if not is_alive:
        print(f"❌ GAGAL TERHUBUNG ke {target_url}!")
        print("   Pastikan server FastAPI (Uvicorn) sudah berjalan:")
        print("   > .\\venv\\Scripts\\python.exe -m uvicorn main:app --reload")
        sys.exit(1)

    print(f"✅ Terhubung ke Server!")
    print(f"   Mode Server Aktif: [{active_mode.upper()}] - {engine_desc}")
    print("-" * 75)
    print("Mulai mengalirkan data... Tekan CTRL+C untuk berhenti.\n")

    patients_pool, pathology_pool = load_datasets()

    count = 0
    next_patient_id = 900001
    start_time = time.time()

    server_process_times = []
    round_trip_times = []

    try:
        while True:
            if max_count > 0 and count >= max_count:
                print(f"\n[SELESAI] Target pengiriman {max_count} data tercapai.")
                break

            now_str = time.strftime("%H:%M:%S")

            send_patient = stream_type in ("patients", "both")
            send_pathology = stream_type in ("pathology", "both")

            # 1. Kirim Data Pasien
            if send_patient:
                if patients_pool:
                    base_row = dict(random.choice(patients_pool))
                else:
                    base_row = {"diagnosis": random.choice(["M", "B"]), "radius_mean": round(random.uniform(10.0, 28.0), 2)}

                base_row["id"] = next_patient_id
                base_row["radius_mean"] = round(float(base_row.get("radius_mean", 18.0)) + random.uniform(-0.5, 0.5), 2)
                next_patient_id += 1

                t0 = time.time()
                try:
                    res = requests.post(f"{target_url}/api/stream/patient", json=base_row, timeout=5.0)
                    rtt_ms = round((time.time() - t0) * 1000, 2)

                    if res.status_code == 200:
                        res_data = res.json()
                        server_ms = res_data.get("ingest_time_ms", 0.0)
                        server_mode = res_data.get("mode", active_mode).upper()

                        server_process_times.append(server_ms)
                        round_trip_times.append(rtt_ms)

                        print(f"[{now_str}] 🩺 Pasien #{base_row['id']} (Diag: {base_row['diagnosis']}, Rad: {base_row['radius_mean']}) -> [{server_mode}] Ingest: {server_ms:6.2f} ms | RTT: {rtt_ms:6.2f} ms")
                    else:
                        print(f"[{now_str}] ⚠️ Gagal kirim pasien: HTTP {res.status_code} - {res.text}")
                except Exception as e:
                    print(f"[{now_str}] ❌ Error kirim pasien: {e}")

                count += 1
                time.sleep(interval)

            # 2. Kirim Data Teks Patologi
            if send_pathology and (max_count <= 0 or count < max_count):
                now_str = time.strftime("%H:%M:%S")
                if pathology_pool:
                    sample = random.choice(pathology_pool)
                    sample_text = sample["text"]
                    sample_name = sample["sample_name"]
                    specialty = sample["medical_specialty"]
                else:
                    sample_name = "Live Pathology Stream"
                    specialty = "Oncology"
                    sample_text = f"Patient presents with tumor examination finding at {now_str}. Malignant carcinoma cells observed."

                payload = {
                    "text": sample_text,
                    "sample_name": sample_name,
                    "medical_specialty": specialty
                }

                t0 = time.time()
                try:
                    res = requests.post(f"{target_url}/api/stream/pathology", json=payload, timeout=5.0)
                    rtt_ms = round((time.time() - t0) * 1000, 2)

                    if res.status_code == 200:
                        res_data = res.json()
                        server_ms = res_data.get("ingest_time_ms", 0.0)
                        server_mode = res_data.get("mode", active_mode).upper()

                        server_process_times.append(server_ms)
                        round_trip_times.append(rtt_ms)

                        summary = (sample_text[:40] + "...") if len(sample_text) > 40 else sample_text
                        print(f"[{now_str}] 📄 Teks: \"{summary}\" -> [{server_mode}] Ingest: {server_ms:6.2f} ms | RTT: {rtt_ms:6.2f} ms")
                    else:
                        print(f"[{now_str}] ⚠️ Gagal kirim teks: HTTP {res.status_code} - {res.text}")
                except Exception as e:
                    print(f"[{now_str}] ❌ Error kirim teks: {e}")

                count += 1
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\n" + "-" * 75)
        print("🛑 Streaming dihentikan oleh pengguna (CTRL+C).")

    total_sec = round(time.time() - start_time, 2)
    avg_server_ms = round(sum(server_process_times) / len(server_process_times), 2) if server_process_times else 0.0
    avg_rtt_ms = round(sum(round_trip_times) / len(round_trip_times), 2) if round_trip_times else 0.0
    min_server_ms = min(server_process_times) if server_process_times else 0.0
    max_server_ms = max(server_process_times) if server_process_times else 0.0

    print("-" * 75)
    print(f"📊 Mode Server           : [{active_mode.upper()}]")
    print(f"📦 Total Data Terkirim   : {count} data")
    print(f"⏱️ Total Waktu Sesi      : {total_sec} detik")
    print(f"⚡ Rata-rata Ingest Server: {avg_server_ms} ms (Min: {min_server_ms} ms, Max: {max_server_ms} ms)")
    print(f"🌐 Rata-rata Total RTT   : {avg_rtt_ms} ms (termasuk latensi jaringan HTTP)")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulator Pengiriman Data Real-Time (Benchmark Durasi Ingest)")
    parser.add_argument("--target", default=DEFAULT_TARGET_URL, help=f"Base URL server FastAPI (default: {DEFAULT_TARGET_URL})")
    parser.add_argument("--interval", type=float, default=1.5, help="Jeda waktu antar pengiriman data dalam detik (default: 1.5)")
    parser.add_argument("--type", choices=["patients", "pathology", "both"], default="both", help="Tipe data yang dikirim (default: both)")
    parser.add_argument("--count", type=int, default=0, help="Jumlah data yang dikirim (0 = infinite streaming sampai di-stop)")
    args = parser.parse_args()

    run_stream(
        target_url=args.target,
        interval=args.interval,
        stream_type=args.type,
        max_count=args.count
    )
