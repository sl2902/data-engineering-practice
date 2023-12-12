import boto3
import requests
import io
import gzip
import time
import shutil

base_url = 'https://data.commoncrawl.org/'
filename = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
url = base_url + filename

def download_first_url(uri: str) -> str:
    resp = requests.get(uri)
    if resp.status_code == 200:
       comp_file = io.BytesIO(resp.content)
       decomp_file = gzip.GzipFile(fileobj=comp_file)
       for link in decomp_file.readlines():
          return link.decode('utf8').strip()
    else:
        raise Exception(f'Error downloading file. Error code {resp.status_code}')

def parse_cc_wet_file(uri: str, output_file: str):
    resp = requests.get(uri, stream=True)
    if resp.status_code == 200:
        decomp_file = gzip.GzipFile(fileobj=resp.raw)
        with open(output_file, 'wb') as f:
            for stream in decomp_file:
                f.write(stream)

def parse_cc_wet_copy_file(uri: str, output_file: str):
    resp = requests.get(uri, stream=True)
    if resp.status_code == 200:
        decomp_file = gzip.GzipFile(fileobj=resp.raw)
        with open(output_file, 'wb') as f:
            shutil.copyfileobj(decomp_file, f)

def main():
    # your code here
    start = time.perf_counter()
    link = download_first_url(url)
    print(link)
    # takes 11 sec
    parse_cc_wet_file(base_url+link, 'cc_file.txt')
    # takes 16 sec
    # parse_cc_wet_copy_file(base_url+link, 'cc_file.txt')
    end = time.perf_counter()
    print(f'Elapsed time {end-start} seconds(s)')


if __name__ == "__main__":
    main()
