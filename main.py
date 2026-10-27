import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pyspark.sql import SparkSession
from hdfs import InsecureClient
from routers import dataframe_api, rdd_api

os.environ["PYSPARK_PYTHON"] = "python"

app = FastAPI(title="Hybrid Spark & Hadoop Dashboard")

# === PAKSA PILIH MODE DI SINI SECARA LANGSUNG ===
# Ubah menjadi "hadoop" atau "spark" tanpa perlu set environment variable di terminal
EXEC_MODE = "spark"  # <-- Ganti jadi "hadoop" jika ingin mencoba mode Hadoop
app.state.execution_mode = EXEC_MODE

if EXEC_MODE == "hadoop":
    print("🐘 Menjalankan mode: HADOOP HDFS")
    try:
        hdfs_client = InsecureClient('http://localhost:9870', user='root')
        app.state.hdfs_client = hdfs_client
        with hdfs_client.read('/data/mtsamples.csv', encoding='utf-8') as reader:
            app.state.hadoop_text_data = reader.readlines()
        print("Berhasil terhubung ke Hadoop HDFS!")
    except Exception as e:
        print(f"⚠️ Gagal terhubung ke Hadoop HDFS: {e}")

elif EXEC_MODE == "spark":
    print("🚀 Menjalankan mode: APACHE SPARK (Single-Thread / Safe Mode)")
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
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def serve_dashboard():
    return FileResponse("static/index.html")


@app.get("/tables")
def serve_tables():
    return FileResponse("static/tables.html")
