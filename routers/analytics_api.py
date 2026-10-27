from collections import defaultdict
from threading import Lock
from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])

OPERATIONS = ("filter_data", "cari_teks")
MODES = ("hadoop", "spark")


def initialise_metrics(app):
    """Create the in-memory measurement store once, when the app starts."""
    app.state.execution_metrics = defaultdict(lambda: defaultdict(list))
    app.state.execution_metrics_lock = Lock()


def record_execution(request: Request, operation: str, execution_time_sec: float) -> None:
    """Store one completed dashboard operation for the active execution engine."""
    mode = getattr(request.app.state, "execution_mode", "spark")
    metrics = request.app.state.execution_metrics
    with request.app.state.execution_metrics_lock:
        metrics[operation][mode].append(float(execution_time_sec))


@router.get("/execution-summary")
def execution_summary(request: Request):
    """Return count, latest, and average durations grouped by operation and engine."""
    metrics = request.app.state.execution_metrics
    summary = {}

    with request.app.state.execution_metrics_lock:
        for operation in OPERATIONS:
            summary[operation] = {}
            for mode in MODES:
                samples = metrics[operation][mode]
                summary[operation][mode] = {
                    "count": len(samples),
                    "average_execution_time_sec": round(sum(samples) / len(samples), 4) if samples else None,
                    "latest_execution_time_sec": round(samples[-1], 4) if samples else None,
                }

    return {
        "active_mode": getattr(request.app.state, "execution_mode", "spark"),
        "operations": summary,
    }
