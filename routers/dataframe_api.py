import time
from fastapi import APIRouter, Request, HTTPException

router = APIRouter(prefix="/api/patients", tags=["DataFrame"])


@router.get("/filter")
def filter_patients(request: Request, min_radius: float = 20.0, diag_type: str = "M"):
    try:
        start_time = time.time()
        mode = getattr(request.app.state, "execution_mode", "spark")

        if mode == "hadoop":
            df = getattr(request.app.state, "hadoop_patients_df", None)
            if df is None:
                raise HTTPException(status_code=500, detail="Data pasien Hadoop belum tersedia.")

            # Filter data berdasarkan diagnosis dan radius_mean minimum
            mask = (df["diagnosis"] == diag_type) & (df["radius_mean"].astype(float) >= float(min_radius))
            filtered = df.loc[mask, ["id", "diagnosis", "radius_mean"]].fillna("")
            data = filtered.to_dict(orient="records")
        else:
            spark = getattr(request.app.state, "spark", None)
            if spark is None:
                raise HTTPException(status_code=500, detail="Spark Session belum diinisialisasi.")

            query = f"SELECT id, diagnosis, radius_mean FROM patients WHERE diagnosis = '{diag_type}' AND radius_mean >= {min_radius}"
            result = spark.sql(query)
            df_pandas = result.toPandas().fillna("")
            data = df_pandas.to_dict(orient="records")

        exec_time = round(time.time() - start_time, 4)
        return {
            "mode": mode,
            "execution_time_sec": exec_time,
            "total_found": len(data),
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
def get_all_patients(request: Request, limit: int = 100):
    try:
        mode = getattr(request.app.state, "execution_mode", "spark")

        if mode == "hadoop":
            df = getattr(request.app.state, "hadoop_patients_df", None)
            if df is None:
                raise HTTPException(status_code=500, detail="Data pasien Hadoop belum tersedia.")

            columns = [c for c in df.columns if not str(c).startswith("Unnamed")]
            subset = df[columns].head(limit).fillna("")
            data = subset.to_dict(orient="records")
            return {"mode": mode, "columns": columns, "data": data}
        else:
            spark = getattr(request.app.state, "spark", None)
            if spark is None:
                raise HTTPException(status_code=500, detail="Spark Session belum diinisialisasi.")

            result = spark.sql(f"SELECT * FROM patients LIMIT {limit}")
            columns = [c for c in result.columns if not str(c).startswith("Unnamed")]
            df_pandas = result.select(*columns).toPandas().fillna("")
            data = df_pandas.to_dict(orient="records")
            return {"mode": mode, "columns": columns, "data": data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
