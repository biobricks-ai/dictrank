# PURPOSE: DOWNLOAD THE SMRT DATA TO THE ./download DIRECTORY
import os 

# downloads to the ./download directory
os.makedirs('download', exist_ok=True)

# read the data from the ./download directory
import requests
source_url = "https://www.fda.gov/media/178811/download?attachment"

# stream the source_url to the ./download/smrt_data.zip
with requests.get(source_url, stream=True) as r:
    r.raise_for_status()
    with open('download/dictrank_dataset.xlsx', 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)