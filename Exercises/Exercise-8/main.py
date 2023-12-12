import duckdb as db

def make_db_connection(database: str = "") -> db.DuckDBPyConnection:
    conn = db.connect(database=database)
    return conn

def create_ddl(conn: db.DuckDBPyConnection, table: str):
    conn.execute("""
                 create table {} (
                    vin varchar,
                    country varchar,
                    city varchar,
                    state varchar,
                    postal_code varchar,
                    model_year bigint,
                    make varchar,
                    model varchar,
                    vehicle_type varchar,
                    cafv varchar,
                    electric_range bigint,
                    base_msrp double,
                    legislative_district varchar,
                    vehicle_id bigint,
                    vehicle_locatopn varchar,
                    electriv_utility varchar,
                    census_tract_2020 bigint
                )""".format(table)
                 
    )

def load_data(conn: db.DuckDBPyConnection, table: str, file: str):
    conn.execute(f"copy {table} from '{file}' (skip 1);")
                 
def count_records(conn: db.DuckDBPyConnection, table: str) -> int:
    conn.execute(f"select count(*) from {table};")
    return conn.fetchall()

def popular_ev(conn: db.DuckDBPyConnection, table: str, n: int=3):
    conn.execute("""
                 select
                    make,
                    max(base_msrp) as highest
                 from
                    {table}
                 group by
                    make
                 order by
                    highest desc
                 limit {n};
                 """.format(table=table, n=n))
    return conn.fetchall()

def popular_ev_by_postalcode(conn: db.DuckDBPyConnection, table: str):
    conn.execute("""
                 select
                   make,
                   postal_code
                 from
                 (
                  select
                     make,
                     postal_code,
                     -- max(base_msrp) as highest
                     row_number() over(partition by postal_code order by base_msrp desc) as rr
                  from
                     {table}
                 ) rank_by_postal
                 where
                     rr = 1;
                 """.format(table=table))
    return conn.fetchall()

def count_ev_by_city(conn: db.DuckDBPyConnection, table: str):
    conn.execute("""
                 select
                    city,
                    count(vin) as cnt
                 from
                    {table}
                 group by
                    city
                 order by
                    cnt desc;
                 """.format(table=table))
    return conn.fetchall()

def write_parquet(conn: db.DuckDBPyConnection, table: str, dest_file: str):
    conn.execute("""
                 copy
                  (select 
                    model_year,
                    count(vin) num_ev
                   from
                    {table}
                   group by
                    model_year
                   order by
                    num_ev desc)
                 to
                    '{dest_file}'
                 (format parquet, partition_by (model_year), overwrite_or_ignore 1)
                 """.format(table=table, dest_file=dest_file))

def main():
    conn = make_db_connection()
    table = "evp_data"
    n = 3
    dest_file = "evp.parquet"
    create_ddl(conn, table)
    load_data(conn, table, "data/Electric_Vehicle_Population_Data.csv")
    num_rows = count_records(conn, table)
    print(f"Number of rows in {table} is {num_rows[0][0]}")
    top3_ev = popular_ev(conn, table, n=n)
    print(f"Top {n} EVs {top3_ev}")
    popular_ev_by_postal = popular_ev_by_postalcode(conn, table)
    print(f"Popular EV by city {popular_ev_by_postal}")
    num_ev_by_city = count_ev_by_city(conn, table)
    print(f'Number of EV by city {num_ev_by_city}')
    write_parquet(conn, table, dest_file)

if __name__ == "__main__":
    main()
