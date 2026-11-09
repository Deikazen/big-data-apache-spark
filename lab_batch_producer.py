"""
Simulasi kedatangan batch hasil laboratorium (Velocity) - SIMULASI, bukan LIS asli.
----------------------------------------------------------------------------------
Membaca data2/lab_results_batch_N.json (JSON Lines) lalu mengirim satu batch per
interval ke POST /api/lab/batches. Server mengukur latensi pemrosesan tiap batch;
producer mengukur round-trip time.

Contoh:
    python lab_batch_producer.py                       # 5 batch, jeda 10 detik
    python lab_batch_producer.py --interval 5 --reset  # kosongkan data lab dulu
    python lab_batch_producer.py --batches 1 2 3 --interval 3
"""
import argparse
import glob
import json
import os
import re
import sys
import time

import requests

DATA2_DIR = os.getenv("DATA2_DIR", "data2")


def batch_files():
    files = glob.glob(os.path.join(DATA2_DIR, "lab_results_batch_*.json"))
    return sorted(files, key=lambda p: int(re.search(r"batch_(\d+)", p).group(1)))


def main():
    ap = argparse.ArgumentParser(description="Simulator kedatangan batch hasil lab")
    ap.add_argument("--target", default="http://localhost:8000")
    ap.add_argument("--interval", type=float, default=10.0, help="jeda antar batch (detik)")
    ap.add_argument("--batches", type=int, nargs="*", help="nomor batch yang dikirim (default: semua)")
    ap.add_argument("--reset", action="store_true", help="kosongkan data lab di server sebelum mulai")
    args = ap.parse_args()

    try:
        mode = requests.get(f"{args.target}/api/mode", timeout=5).json()
    except requests.RequestException as e:
        sys.exit(f"[ERROR] Server tidak dapat dihubungi di {args.target}: {e}")
    print(f"[INFO] Server aktif - mode: {mode.get('mode')} ({mode.get('engine')})")

    files = batch_files()
    if not files:
        sys.exit(f"[ERROR] Tidak ada lab_results_batch_*.json di {DATA2_DIR}/")
    if args.batches:
        files = [f for f in files if int(re.search(r"batch_(\d+)", f).group(1)) in args.batches]

    if args.reset:
        requests.post(f"{args.target}/api/lab/reset", timeout=30)
        print("[INFO] Data lab di server dikosongkan.")

    print(f"[INFO] Mengirim {len(files)} batch, interval {args.interval}s (SIMULASI kedatangan periodik)\n")
    print(f"{'batch':>5} {'records':>8} {'dedup':>6} {'server_ms':>10} {'rec/s':>9} {'e2e_ms':>9} {'roundtrip_ms':>13}")
    for i, path in enumerate(files):
        n = int(re.search(r"batch_(\d+)", path).group(1))
        with open(path, encoding="utf-8") as fh:
            records = [json.loads(line) for line in fh if line.strip()]
        sent_at = time.time()
        t0 = time.perf_counter()
        res = requests.post(f"{args.target}/api/lab/batches", timeout=600,
                            json={"batch_id": n, "sent_at": sent_at, "records": records})
        rtt = (time.perf_counter() - t0) * 1000
        if res.status_code != 200:
            print(f"[ERROR] batch {n}: HTTP {res.status_code} {res.text[:200]}")
            continue
        b = res.json()["batch"]
        print(f"{n:>5} {b['records']:>8} {b['duplicates_removed']:>6} {b['processing_latency_ms']:>10} "
              f"{b['throughput_rec_per_sec']:>9} {b['end_to_end_latency_ms']:>9} {rtt:>13.1f}")
        if i < len(files) - 1:
            time.sleep(args.interval)
    print("\n[DONE] Lihat ringkasan di http://localhost:8000/lab atau GET /api/lab/batches")


if __name__ == "__main__":
    main()
