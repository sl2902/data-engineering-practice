import zipfile
import os
import glob
from pathlib import Path
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
                                .appName('Divvy rides')
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

def transform_fields(df: SparkDataFrame):
    df = df.withColumnRenamed('started_at', 'start_time')
    df = df.withColumnRenamed('ended_at', 'end_time')
    df = df.withColumnRenamed("start_station_id", "from_station_id")
    df = df.withColumnRenamed("start_station_name", "from_station_name")
    df = df.withColumnRenamed("end_station_id", "to_station_id")
    df = df.withColumnRenamed("end_station_name", "to_station_name")
    df = df.withColumn('start_time', to_timestamp(col('start_time')))
    df = df.withColumn('end_time', to_timestamp(col('end_time')))
    df = df.withColumn('duration', (col('end_time') - col('start_time')).cast(LongType()))
    df = df.withColumn('date', to_date(col('start_time')))
    df = df.withColumn('day', dayofmonth(col('start_time')))
    df = df.withColumn('month', month(col('start_time')))
    df = df.withColumn("dow", dayofweek(col("date")))
    df = df.withColumn("dow2", date_format(col("date"), "F"))
    df = df.withColumn("quarter", date_format(col("date"), "Q"))
    df = df.withColumn("str_week", ceil(col("day") / 7))
    return df

def write_report(df: SparkDataFrame, out_path: str, report_name: str):
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", True)\
        .csv(f"{out_path}/{report_name}")

def avg_trip_duration(df: SparkDataFrame):
    return df.groupBy('day').agg(avg(col('duration'))).orderBy('day')

def num_trips_per_day(df: SparkDataFrame):
    return df.groupBy('day').agg(count("*")).orderBy('day')

def most_popular_starting_station(df: SparkDataFrame):
    df = df\
        .groupBy(["month", "from_station_id", "from_station_name"])\
        .agg(count("*").alias("num_stations"))
        # .orderBy(["month", "num_stations"], ascending=[True, False])
    window_spec = Window.partitionBy("month").orderBy(col("num_stations").desc())
    return df.withColumn("station_rank", dense_rank()\
                         .over(window_spec))\
                         .filter(col("station_rank") == 1)\
                         .select(col("month"), 
                                 col("from_station_id"), 
                                 col("from_station_name"), 
                                 col("num_stations")
                                )

def top_3_starting_station(df: SparkDataFrame):

    df = df.withColumn("last_day", expr("""
                                        to_date(
                                            date_trunc('quarter', date + interval 3 months)
                                            ) - interval 1 day
                                        """))\
        .withColumn("cut_off", col("last_day") - timedelta(days=14))\
        .filter(col("date") > col("cut_off"))\
        .groupBy(["date", "from_station_id", "from_station_name"])\
        .agg(count("*").alias("num_stations"))
    window_spec = Window.partitionBy("date").orderBy(col("num_stations").desc())

    return df.withColumn("station_rank", dense_rank()\
                         .over(window_spec))\
                         .filter(col("station_rank") < 4)\
                         .select(col("date"), 
                                 col("from_station_id"), 
                                 col("from_station_name"), 
                                 col("num_stations")
                                )

def male_vs_female_avg_trips(df: SparkDataFrame):
    if "gender" in df.columns:
        df = df\
            .filter(col("gender").isNotNull())\
            .groupBy(["gender"])\
            .agg(avg("duration").alias("avg_duration"))\
            .orderBy(["avg_duration"], ascending=[False])
        return df

def top10_longest_and_shortest_duration(df: SparkDataFrame, n: int=10):
    if "birthyear" in df.columns:
        df = df\
            .filter(col("birthyear").isNotNull())\
            .withColumn("age", 2023-col("birthyear"))\
            .groupBy(["age"])\
            .agg(sum("duration").alias("total_duration"))\
            .orderBy("total_duration", ascending=True)
        
        shortest = df\
                    .withColumn("category", lit("shortest"))\
                    .limit(n)
        longest = df\
                    .withColumn("category", lit("Longest"))\
                    .orderBy(col("total_duration"), ascending=False)\
                    .limit(n)
            
        return shortest.union(longest)
        
        # window_longest = Window.partitionBy().orderBy(col("total_duration").desc())
        # window_shortest = Window.partitionBy("age").orderBy(col("total_duration"))
        # return df.withColumn("longest_duration", dense_rank()\
        #     .over(window_longest))\
        #     .filter(col("longest_duration") <= 10)\
        #     .select(lit("Longest").alias("category"), col("age"), col("total_duration"), col("longest_duration"))


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
            df = transform_fields(df)
            result = avg_trip_duration(df)
            write_report(result, f"report/{folder}", "avg_trip_duration")
            result = num_trips_per_day(df)
            write_report(result, f"report/{folder}", "num_trips_per_day")
            result = most_popular_starting_station(df)
            write_report(result, f"report/{folder}", "most_popular_from_station")
            result = top_3_starting_station(df)
            write_report(result, f"report/{folder}", "top3_station")
            result = male_vs_female_avg_trips(df)
            if result:
                write_report(result, f"report/{folder}", "male_vs_female_avg_duration")
            if result:
                result = top10_longest_and_shortest_duration(df)
                write_report(result, f"report/{folder}", "top10_longest_and_shortest_duration")
            try:
                path = Path(f"{file}")
                path.unlink()
            except os.error as e:
                print(f"Error deleting files {e}")
        



if __name__ == "__main__":
    main()
