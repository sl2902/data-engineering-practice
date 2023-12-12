import requests
import zipfile
import io
import os
from pathlib import Path
from typing import List
import time

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# def download_files(uris: List, path: str):
#     dir_path = Path(path)
#     dir_path.mkdir(parents=True, exist_ok=True)
#     for uri in uris:
#         try:
#             resp = requests.get(uri)
#         except Exception as e:
#             print(f"Invalid uri {uri}. Skipping...")
#             print(f'{e}')
#         if resp.status_code == 200:
#             zf = zipfile.ZipFile(io.BytesIO(resp.content))
#             zf.extractall(dir_path)

def make_request(uri: str, download_path: str):
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

def download_files(zip_file: str, dir_path: str):
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
    for uri in download_uris:
        filename = make_request(uri, 'download/')
        if filename is not None:
            download_files(filename, 'download/')
            delete_zips(filename)
    end = time.perf_counter()
    print(f'Elapsed time {end-start} second(s)')


if __name__ == "__main__":
    main()
