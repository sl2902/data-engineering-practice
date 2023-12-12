import zipfile
import os
import glob
import re
from datetime import timedelta
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import *


def create_spark_session():
    """
    Build a Pyspark session
    Returns - A Pyspark object
    """
    try:
        spark = (
                    SparkSession.builder
                                .appName('Exercise7')
                                .enableHiveSupport()
                                .getOrCreate()
        )
    except Exception as e:
        raise Exception('Pyspark session failed to be created...')
    return spark

def extract_zip_folder(zip_file: str, out_dir: str):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_file:
            zip_file.extractall(out_dir)
    except Exception as e:
        raise Exception(f"Zip file deflation failed with {e}")

def transform_fields(df: SparkDataFrame, filename: str):
   df = df.withColumn("source_file", lit(filename))
   pat = re.compile(r'(\d{4}-\d{2}-\d{2})')
   match = pat.search(filename)
   if match:
       file_date = match.group(0)
       df = df.withColumn("file_date", lit(file_date).cast(DateType()))
   df = df\
        .withColumn("brand", when(col("model").contains(" "), split(col("model"), " ")[0])\
        .otherwise("unknown"))
   df2 = df\
        .withColumn("brand", when(col("model").contains(" "), split(col("model"), " ")[0])\
        .otherwise("unknown"))\
        .drop("model")
        # .groupBy("brand").agg(sum("capacity_bytes"))
   
   window = Window.partitionBy("brand").orderBy(col("capacity_bytes").desc())
   df2 = df2\
    .withColumn("storage_ranking", dense_rank()\
                .over(window))
   df = df.join(df2.select(col("brand"), col("storage_ranking")), on="brand", how="inner")
   df = df.withColumn("hash", md5(concat(coalesce(col("serial_number"), lit(""), col("brand"), lit("")))))
                
   return df.select(col("source_file"), 
                    col("file_date"), 
                    col("serial_number"),
                    col("brand"), 
                    col("capacity_bytes"), 
                    col("storage_ranking"), 
                    col("hash"))
       

def main():
    # your code here
    zip_dir = 'data/*'
    out_dir = '/tmp/'
    spark = create_spark_session()
    for file in glob.glob(zip_dir):
        if file.endswith(".zip"):
            extract_zip_folder(file, out_dir)

    for file in glob.glob(f"{out_dir}/*"):
        filename = file.split("/")[-1]
        folder = file.split("/")[-1].split(".")[0]
        if file.endswith(".csv"):
            df = spark.read.option("header", True).csv(file)
            df = transform_fields(df, filename=filename)
            print(df.show())      



if __name__ == "__main__":
    main()
