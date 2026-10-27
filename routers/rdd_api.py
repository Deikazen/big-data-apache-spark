import time
from fastapi import APIRouter, Request, HTTPException
from routers.analytics_api import record_execution

router = APIRouter(prefix="/api/pathology", tags=["RDD"])


@router.get("/search")
def search_keyword(request: Request, keyword: str = "cancer"):
    try:
        start_time = time.time()
        mode = getattr(request.app.state, "execution_mode", "spark")
        target_word = keyword.lower()

        if mode == "hadoop":
            all_lines = getattr(request.app.state, "hadoop_text_data", None)
            if all_lines is None:
                raise HTTPException(status_code=500, detail="Data teks Hadoop belum tersedia.")
        else:
            text_rdd = getattr(request.app.state, "text_rdd", None)
            if text_rdd is None:
                raise HTTPException(status_code=500, detail="Spark text_rdd belum diinisialisasi.")
            # Tarik data dengan aman menggunakan collect() untuk menghindari crash di Windows
            all_lines = text_rdd.collect()

        matched_lines = [
            str(line) for line in all_lines if target_word in str(line).lower()
        ]

        lines = matched_lines[:50]
        safe_lines = [
            line.encode("utf-8", "ignore").decode("utf-8") for line in lines
        ]

        exec_time = round(time.time() - start_time, 4)
        record_execution(request, "cari_teks", exec_time)
        return {
            "mode": mode,
            "execution_time_sec": exec_time,
            "keyword": target_word,
            "data": safe_lines
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
def get_all_pathology(request: Request, limit: int = 100):
    try:
        mode = getattr(request.app.state, "execution_mode", "spark")

        if mode == "hadoop":
            all_lines = getattr(request.app.state, "hadoop_text_data", None)
            if all_lines is None:
                raise HTTPException(status_code=500, detail="Data teks Hadoop belum tersedia.")
            lines = all_lines[:limit]
        else:
            text_rdd = getattr(request.app.state, "text_rdd", None)
            if text_rdd is None:
                raise HTTPException(status_code=500, detail="Spark text_rdd belum diinisialisasi.")
            try:
                lines = text_rdd.take(limit)
            except Exception:
                # Fallback aman jika python worker mengalami socket reset di Windows
                all_lines = text_rdd.collect()
                lines = all_lines[:limit]

        safe_lines = [
            str(line).encode("utf-8", "ignore").decode("utf-8") for line in lines
        ]
        return {"mode": mode, "data": safe_lines}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
