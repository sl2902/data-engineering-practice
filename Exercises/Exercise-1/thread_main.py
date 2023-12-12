import requests
import zipfile
import io
import os
from pathlib import Path
from typing import List
import time
import concurrent.futures

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def make_request(uri: str, download_path: str = 'download/'):
    dir_path = Path(download_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(uri)
    except Exception as e:
        print(f"Invalid uri {uri}. Skipping...")
        print(f'{e}')
    if resp.status_code == 200:
        filename = os.path.join(download_path, uri.split("/")[-1])
        with open(filename, 'wb') as f:
            f.write(resp.content)
        return filename
    else:
        print(f'Response code is not 200 but {resp.status_code}')

def download_files(zip_file: str, dir_path: str = 'download/'):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            zf.extractall(dir_path)
    except Exception as e:
        print(f'{e}')

def delete_zips(zip_file: str):
    try:
        path = Path(zip_file)
        path.unlink()
    except os.error as e:
        print(f'Failed to delete file {path}')


def main():
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor() as exe:
        # exe.map() returns a generator which is exhausted
        # in the event of an error. so use exe.submit() instead
        #results = exe.map(make_request, download_uris)
        futures = [
            exe.submit(make_request, uri) 
            for uri in download_uris
        ]
        df_fut = [
                    exe.submit(download_files, future.result())
                    for future in futures
                ]
        for df in df_fut: 
            try:
                df.result()
            except Exception as e:
                pass
        dz_fut = [
                    exe.submit(delete_zips, future.result())
                    for future in futures
                ]
        for df in dz_fut: 
            try:
                df.result()
            except Exception as e:
                pass
    end = time.perf_counter()
    print(f'Elapsed time {end-start} second(s)')


if __name__ == "__main__":
    main()