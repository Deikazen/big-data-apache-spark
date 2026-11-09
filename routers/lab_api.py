"""
Router Clinical Laboratory Data Analytics (data2/).

Pemetaan endpoint -> 5V:
    GET  /api/lab/overview              Volume   (jumlah record, ukuran file, relasi antar tabel)
    POST /api/lab/load-all              Volume   (bulk load historis semua batch + ukuran waktu)
    POST /api/lab/batches               Velocity (batch hasil lab datang periodik, diukur latensinya)
    GET  /api/lab/batches               Velocity (log kedatangan, inter-arrival, latensi)
    GET  /api/lab/quality               Veracity (before/after masalah kualitas data)
    GET  /api/lab/analysis/simple       Value    (Operasi A: agregasi sederhana)
    GET  /api/lab/analysis/relational   Value    (Operasi B: join + temporal)
    GET  /api/lab/analysis/compare      Volume   (kompleksitas A vs B, rata-rata beberapa kali jalan)
    GET  /api/lab/preview               Variety  (record terintegrasi + quality_flag)
    POST /api/lab/reset                 utilitas demo
"""
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from routers.analytics_api import record_execution

router = APIRouter(prefix="/api/lab", tags=["Lab Analytics"])


class LabBatchPayload(BaseModel):
    batch_id: Optional[int] = None
    sent_at: Optional[float] = None          # epoch detik di sisi producer (untuk end-to-end latency)
    records: List[Dict[str, Any]]            # satu record = satu baris JSON Lines dari data2/


def _store(request: Request):
    store = getattr(request.app.state, "lab_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Lab store belum siap. Periksa log startup dan folder data2/.")
    return store


def _guard(fn):
    try:
        return fn()
    except HTTPException:
        raise
    except (RuntimeError, ValueError, FileNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overview")
def overview(request: Request):
    store = _store(request)
    return _guard(store.overview)


@router.post("/load-all")
def load_all(request: Request):
    store = _store(request)
    start = time.time()

    def run():
        result = store.load_all()
        record_execution(request, "lab_ingest", time.time() - start)
        return result
    return _guard(run)


@router.post("/batches")
def ingest_batch(payload: LabBatchPayload, request: Request):
    store = _store(request)
    if not payload.records:
        raise HTTPException(status_code=400, detail="records kosong.")
    start = time.time()

    def run():
        entry = store.ingest("records", payload.records, batch_id=payload.batch_id,
                             source="producer", sent_at=payload.sent_at)
        record_execution(request, "lab_ingest", time.time() - start)
        return {"status": "success", "batch": entry}
    return _guard(run)


@router.get("/batches")
def batch_log(request: Request):
    return _store(request).batches()


@router.get("/quality")
def quality(request: Request):
    return _guard(_store(request).quality)


@router.get("/analysis/simple")
def analysis_simple(request: Request):
    store = _store(request)

    def run():
        res = store.simple_agg()
        record_execution(request, "lab_simple_agg", res["execution_time_sec"])
        return res
    return _guard(run)


@router.get("/analysis/relational")
def analysis_relational(request: Request):
    store = _store(request)

    def run():
        res = store.relational()
        record_execution(request, "lab_relational", res["execution_time_sec"])
        return res
    return _guard(run)


@router.get("/analysis/compare")
def analysis_compare(request: Request, repeats: int = 3):
    store = _store(request)
    return _guard(lambda: store.compare(repeats))


@router.get("/preview")
def preview(request: Request, limit: int = 100, flag: Optional[str] = None):
    store = _store(request)
    return _guard(lambda: store.preview(limit, flag))


@router.post("/reset")
def reset(request: Request):
    store = _store(request)
    store.reset()
    return {"status": "reset", "message": "Data lab dikosongkan. Encounters tetap dimuat."}
