# PURPOSE: CHANGE THE DOWNLOADED DATA TO ONE OR MORE PARQUET FILES
import os 

# exports to the ./brick directory
os.makedirs('brick', exist_ok=True)

# read the data from the ./download directory
