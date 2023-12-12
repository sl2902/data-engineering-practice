import polars as pl

def read_pl(filename: str):
    q = pl.scan_csv(filename)
    # q.select(
    #     pl.col('started_at').str.to_datetime('%Y-%m-%d %H:%M:%S'),
    #     pl.col('ended_at').str.to_datetime('%Y-%m-%d %H:%M:%S'),
    #     pl.col('end_station_id').cast(pl.Utf8)
    # )
    q = q.with_columns(pl.col('started_at').str.to_datetime('%Y-%m-%d %H:%M:%S'))
    q = q.with_columns(pl.col('ended_at').str.to_datetime('%Y-%m-%d %H:%M:%S'))
    q = q.with_columns(pl.col('end_station_id').cast(pl.Utf8))
    # print(q.schema)
    return q
    # conn = pl.SQLContext(trip=q, eager_execution=True)
    # return conn

def transform_1(q):
    # Count the number bike rides per day.
   res =  q.with_columns(pl.col('started_at').dt.day().alias('day'))\
        .sort('day')\
        .group_by('day', maintain_order=True)\
        .count()
   return res.collect()

def transform_2(q):
    # Calculate the average, max, and minimum number of rides per week of the dataset.
    tmp = q.select(
        pl.col('started_at').dt.week().alias('week'),
        pl.col('started_at').dt.day().alias('day')
        )
    res = tmp.group_by('week', maintain_order=True)\
    .agg(pl.col('day').mean().alias('mean'),
         pl.col('day').min().alias('min'),
         pl.col('day').max().alias('max'))\
    .sort('week')
    return res.collect()

def transform_3(q):
    # For each day, calculate how many rides that day is above or below the same day last week.
    # my intrepretation of this question is that we should compute the weekly lag of ride count
    tmp = q.select(
        pl.col('started_at').dt.week().cast(pl.Int32).alias('week'),
        pl.col('started_at').dt.weekday().cast(pl.Int32).alias('weekday')
    )
    res = tmp.group_by(['week', 'weekday'], maintain_order=True)\
        .count()\
        .cast(pl.Int32)\
        .sort(['week', 'weekday'])
    res = res.select(
        pl.all(),
        pl.col('count').shift().over('weekday').alias('lag_rides')
    )
    res = res.with_columns(diff=pl.col('count') - pl.col('lag_rides'))
    return res.collect()

def main():
    filepath = 'data/202306-divvy-tripdata.csv'
    df = read_pl(filepath)
    num_bike_rides_per_day = transform_1(df)
    print(f'Number of bike rides per day {num_bike_rides_per_day}')
    min_max_avg_rides_per_week = transform_2(df)
    print(f'Min, Max and Avg rides rides per week {min_max_avg_rides_per_week}')
    res = transform_3(df)
    print(res)


if __name__ == "__main__":
    main()
