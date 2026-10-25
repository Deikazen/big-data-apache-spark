import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pyspark.sql import SparkSession
from routers import dataframe_api, rdd_api

os.environ["PYSPARK_PYTHON"] = "python"

app = FastAPI(title="Spark Analytics Dashboard")

spark = (SparkSession.builder
         .appName("CancerDataDashboard")
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
