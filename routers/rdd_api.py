import time
from fastapi import APIRouter, Request, HTTPException

router = APIRouter(prefix="/api/pathology", tags=["RDD"])


@router.get("/search")
def search_keyword(request: Request, keyword: str = "cancer"):
    try:
        start_time = time.time()
        text_rdd = request.app.state.text_rdd
        target_word = keyword.lower()

        # Tarik data dengan aman menggunakan collect() untuk menghindari crash di Windows
        all_lines = text_rdd.collect()
        matched_lines = [
            str(line) for line in all_lines if target_word in str(line).lower()]

        lines = matched_lines[:50]
        safe_lines = [line.encode("utf-8", "ignore").decode("utf-8")
                      for line in lines]

        exec_time = round(time.time() - start_time, 4)
        return {"execution_time_sec": exec_time, "keyword": target_word, "data": safe_lines}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
def get_all_pathology(request: Request, limit: int = 100):
    try:
        text_rdd = request.app.state.text_rdd
        # Menggunakan take() dengan pengaman konversi string
        lines = text_rdd.take(limit)
        safe_lines = [str(line).encode(
            "utf-8", "ignore").decode("utf-8") for line in lines]
        return {"data": safe_lines}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
