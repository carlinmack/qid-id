import json
import time

import pandas as pd
import requests
from tqdm import tqdm


data_dir = "crossref/data/"

with open(data_dir + "cc.json", "r") as r:
    LICENSES = json.load(r)


def licenseToCC(url):
    url = url.strip().lower().replace("http://", "https://")
    if url[-1:] == "/":
        w = url
        wo = url[:-1]
    else:
        w = url + "/"
        wo = url
    if w in LICENSES:
        return LICENSES[w]
    elif wo in LICENSES:
        return LICENSES[wo]
    else:
        with open("no-qid-for-license.txt", "a") as a:
            a.write(url + "\n")
        return None

tick = time.time()

# shorter delays in pinging the API
email = "carlin.mackenzie@gmail.com"

# Get data
df = pd.read_csv("invasion-bio.csv")
df["licenseQID"] = ""
df["licenseURL"] = ""
# dois = df["doi"].values.tolist()

# ite = 0

# Keep retrieving pages of results until there are no more pages
for row in tqdm(df.itertuples()):
    if pd.isnull(row.wdLicenseQID):
        url = f"https://api.crossref.org/works/{row.doi}"

        # Set up the initial request
        response = requests.get(url, headers={"User-Agent": f"mailto:{email}"})

        # Parse the JSON response
        if response.status_code == 200:
            data = response.json()

            if "license" in data["message"]:
                licenses = []
                if len(data["message"]["license"]) == 1:
                    license = data["message"]["license"][0]
                    # print(license)
                    df.at[row.Index, 'licenseURL'] = license["URL"]
                    df.at[row.Index, 'licenseQID'] = licenseToCC(license["URL"])

        # Get the rate limit information from the headers
        limit = int(response.headers["x-rate-limit-limit"])
        interval = response.headers["x-rate-limit-interval"]

        # Calculate the time delay between requests
        delay = 1 / limit
        time.sleep(delay)

    # ite += 1
    # if ite > 10:
    #     break

print(f"Elapsed time: {time.time() - tick} seconds")

df.to_csv("doiToLicence.csv", index=False)
