import requests
from tqdm import tqdm

with open("missing-dois.txt") as f:
    inputList = [line.strip() for line in f]

base_url = "https://doi.org/"
validDois = []
invalidDois = []

for doi in tqdm(inputList):
    response = requests.get(base_url + doi, allow_redirects=False)

    if response.status_code == 301 or response.status_code == 302:
        validDois.append(doi)
    elif response.status_code == 404:
        invalidDois.append(doi)
    else:
        print(doi, response.status_code)
    

with open("valid-missing-dois.txt", "w") as w:
        w.write('\n'.join(validDois))

with open("invalid-missing-dois.txt", "w") as w:
        w.write('\n'.join(invalidDois))
