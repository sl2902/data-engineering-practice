import json
import os
import glob
import pandas as pd
import csv
from typing import List, Dict

def search_json_files(file_path: str) -> List[str]:
    json_files = [
        file 
        for file in glob.iglob(file_path, recursive=True)
    ]
    return json_files

def flatten_json(d, name='', exclude=[''], sep='_'):
  """
  credit
  https://stackoverflow.com/questions/52795561/flattening-nested-json-in-pandas-data-frame
  """
  out = {}
  def flatten(d, name=name):
    if isinstance(d, dict):
      for a in d:
        if a not in exclude:
          flatten(d[a], name + a + sep)
    elif isinstance(d, list):
      i = 0
      for ele in d:
        flatten(ele, name + str(i) + sep)
        i += 1
    else:
      out[name[:-1]] = d
  flatten(d)
  return out

def process_json(file) -> Dict:
    with open(file, 'r') as f:
        data = json.load(f)
    return flatten_json(data)

def write_csv(filename: str, data: Dict):
   with open(filename, 'w') as f:
      writer = csv.DictWriter(f, delimiter=",", fieldnames=data.keys())
      writer.writerow({f: f for f in data})
      writer.writerow(data)
        

def main():
   files =  search_json_files('data/**/*.json')
   for file in files:
      write_csv(f"{file.split('.')[0]}.csv", 
                process_json(file))


if __name__ == "__main__":
    main()
