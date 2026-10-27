import os
import pandas as pd
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from routers import analytics_api, dataframe_api, rdd_api

os.environ["PYSPARK_PYTHON"] = "python"

app = FastAPI(title="Hybrid Spark & Hadoop Dashboard")
analytics_api.initialise_metrics(app)

# === PILIH MODE DI SINI SECARA LANGSUNG ===
# Ubah nilai di bawah menjadi "hadoop" atau "spark"
# Nilai ini juga dapat di-override lewat environment variable EXEC_MODE
EXEC_MODE = os.getenv("EXEC_MODE", "hadoop").lower()
app.state.execution_mode = EXEC_MODE

if EXEC_MODE == "hadoop":
    print(f"[MODE] Menjalankan mode: HADOOP HDFS")
    try:
        from hdfs import InsecureClient
        hdfs_client = InsecureClient('http://localhost:9870', user='root')
        app.state.hdfs_client = hdfs_client

        # Membaca data laporan patologi dari HDFS
        with hdfs_client.read('/data/mtsamples.csv', encoding='utf-8') as reader:
            app.state.hadoop_text_data = reader.readlines()

        # Membaca data pasien kanker dari HDFS ke pandas DataFrame
        with hdfs_client.read('/data/breast_cancer_data.csv', encoding='utf-8') as reader:
            app.state.hadoop_patients_df = pd.read_csv(reader)

        print("[HADOOP] Berhasil terhubung ke Hadoop HDFS dan memuat dataset!")
    except Exception as e:
        print(f"[WARN] Gagal membaca dari Hadoop HDFS: {e}")
        if os.path.exists("data/breast_cancer_data.csv") and os.path.exists("data/mtsamples.csv"):
            print("[FALLBACK] Menggunakan dataset lokal untuk mode Hadoop...")
            with open("data/mtsamples.csv", encoding="utf-8", errors="ignore") as f:
                app.state.hadoop_text_data = f.readlines()
            app.state.hadoop_patients_df = pd.read_csv(
                "data/breast_cancer_data.csv")
        else:
            app.state.hadoop_text_data = []
            app.state.hadoop_patients_df = pd.DataFrame()

elif EXEC_MODE == "spark":
    print(f"[MODE] Menjalankan mode: APACHE SPARK (Single-Thread / Safe Mode)")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName("CancerDataDashboard")
             # Dikunci 1 thread agar tidak crash/gacha di Windows
             .master("local[1]")
             # Matikan Arrow penyebab error pandas
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .getOrCreate())

    app.state.spark = spark
    app.state.text_rdd = spark.sparkContext.textFile("data/mtsamples.csv")

    df = spark.read.csv("data/breast_cancer_data.csv",
                        header=True, inferSchema=True)
    df.createOrReplaceTempView("patients")

app.include_router(dataframe_api.router)
app.include_router(rdd_api.router)
app.include_router(analytics_api.router)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/api/mode")
def get_mode():
    mode = getattr(app.state, "execution_mode", "spark")
    return {
        "mode": mode,
        "engine": "Hadoop HDFS" if mode == "hadoop" else "Apache Spark (SQL/RDD)"
    }


@app.get("/")
def serve_dashboard():
    return FileResponse("static/index.html")


@app.get("/tables")
def serve_tables():
    return FileResponse("static/tables.html")
