# PURPOSE: CHANGE THE DOWNLOADED DATA TO ONE OR MORE PARQUET FILES
import os, shutil, pandas as pd

# exports to the ./brick directory
os.makedirs('brick', exist_ok=True)

# read the sheets from ./download/dictrank_dataset.xlsx
xls = pd.ExcelFile('./download/dictrank_dataset.xlsx')

assert xls.sheet_names == ['Table S1']
df = pd.read_excel(xls, 'Table S1') # there is only one sheet
df.to_parquet('brick/smrt_dataset.parquet')

# check that there are the same number of rows and columns in df and the parquet
df2 = pd.read_parquet('brick/smrt_dataset.parquet')
assert df.shape == df2.shape