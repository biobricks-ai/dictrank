# PURPOSE: CHECK IF THE SOURCE HAS CHANGED
# PURPOSE: CHECK IF THE SOURCE HAS CHANGED
import requests
source_url = "https://www.fda.gov/science-research/bioinformatics-tools/drug-induced-cardiotoxicity-rank-dictrank-dataset"

# request.get source_url to status.txt
response = requests.get(source_url)

with open("status.txt", "w") as file:
    file.write(str(response.text))