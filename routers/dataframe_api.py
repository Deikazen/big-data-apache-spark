import time
from fastapi import APIRouter, Request, HTTPException

router = APIRouter(prefix="/api/patients", tags=["DataFrame"])


@router.get("/filter")
def filter_patients(request: Request, min_radius: float = 20.0, diag_type: str = "M"):
    try:
        start_time = time.time()
        spark = request.app.state.spark

        query = f"SELECT id, diagnosis, radius_mean FROM patients WHERE diagnosis = '{diag_type}' AND radius_mean >= {min_radius}"
        result = spark.sql(query)
        df_pandas = result.toPandas().fillna("")
        data = df_pandas.to_dict(orient="records")

        exec_time = round(time.time() - start_time, 4)
        return {"execution_time_sec": exec_time, "total_found": len(data), "data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
def get_all_patients(request: Request, limit: int = 100):
    try:
        spark = request.app.state.spark

        result = spark.sql(f"SELECT * FROM patients LIMIT {limit}")
        columns = result.columns
        df_pandas = result.toPandas().fillna("")
        data = df_pandas.to_dict(orient="records")

        return {"columns": columns, "data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
