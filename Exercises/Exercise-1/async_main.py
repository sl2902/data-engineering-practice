import requests
import zipfile
import io
import os
from pathlib import Path
from typing import List
import aiohttp
import asyncio
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

async def make_request(uri: str, destination_path: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as resp:
            try:
                if resp.status == 200:
                    dir_path = Path(destination_path)
                    dir_path.mkdir(parents=True, exist_ok=True)
                    filename = os.path.join(destination_path, uri.split("/")[-1])
                    with open(filename, 'wb') as f:
                        f.write(await resp.read())
                    return filename
                else:
                    print(f'uri {uri} returned with status {resp.status}. Skipping...')
            except Exception as e:
                print(f"uri {uri} failed with error {e}. Skipping...")

async def download_files(zip_file: str, dir_path: str):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            zf.extractall(dir_path)
    except Exception as e:
        print(f'{e}')

async def delete_zips(filepath: str):
    try:
        path = Path(filepath)
        path.unlink()
    except os.error as e:
        print(f'Failed to delete file {path}')

async def download_extract_delete(uri: str, path: str = 'download/'):
    filename = await make_request(uri, path)
    if filename is not None and os.path.isfile(filename):
        await download_files(filename, path)
        await delete_zips(filename)


async def main():
    start = time.perf_counter()
    tasks = [download_extract_delete(uri) for uri in download_uris]
    await asyncio.gather(*tasks)
    end = time.perf_counter()
    print(f'Elapsed time {end-start} second(s)')



if __name__ == "__main__":
    asyncio.run(main())