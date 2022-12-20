import argparse
import csv
import gzip
import json
import math
import os
import time

import pandas as pd
import tqdm

import id_to_qid

dataDir = "crossref/data/"

with open(dataDir + "cc.json", "r") as r:
    licenses = json.load(r)


def main(batch):
    directory = dataDir + "April 2022 Public Data File from Crossref/"

    output = []
    # it = 0

    for filename in tqdm.tqdm(os.listdir(directory)):
        f = os.path.join(directory, filename)

        try:
            with gzip.open(f, "r") as r:
                data = json.loads(r.read().decode("utf-8"))
        except:
            print("😳")
            print(f)

        for o in data:
            for i in data[o]:
                if "license" in i:
                    if len(i["license"]) == 1:
                        k = i["license"][0]
                        if "creativecommons" in k["URL"]:                            
                            qid = licenseToCC(k["URL"])
                            start, end = getStartEnd(k)
                            output.append([i["DOI"], k["URL"], qid, start, end])

        # it += 1
        # if it > 25:
        #     break


    if not batch:
        filename = dataDir + "CC.csv"
    else:
        filename = dataDir + "CC-" + batch + ".csv"
    with open(filename, "w", newline="") as w:
        w.write("doi,licenseURL,licenseQID,start,end\n")
        writer = csv.writer(w)
        writer.writerows(output)

    # df = pd.DataFrame(output)
    # licences = df.iloc[:, 1]
    # licences.drop_duplicates(inplace=True)
    # licences.to_csv(dataDir + "licenses.csv", index=False)


def getIDs(batch):
    if batch:
        file = dataDir + f"CC-{batch}.csv"
    else:
        file = dataDir + "CC.csv"

    df = pd.read_csv(file)
    dois = df.iloc[:, 0]
    dois = dois.values.tolist()

    id_to_qid.main(inputList=dois, batch=batch)


def collate(batch):
    if not batch:
        df1 = pd.read_csv(dataDir + "CC.csv")
        df2 = pd.read_csv(dataDir + "qid-doi.csv")
    else:
        df1 = pd.read_csv(dataDir + f"CC-{batch}.csv")
        df2 = pd.read_csv(dataDir + f"qid-doi-{batch}.csv")
    result = pd.merge(df1, df2, on="doi")
    
    alreadyLicensed = result.loc[result['wdLicenseQID'].notnull()]
    alreadyLicensed.to_csv(dataDir + "alreadyLicensed.csv", index=False)

    nonLicensed = result.loc[result['wdLicenseQID'].isnull()]

    filenameQs = dataDir + "qs.csv"
    filenameCs = dataDir + "qs-copyright-status.csv"
    if batch:
        filenameQs = dataDir + "qs" + batch + ".csv"
        filenameCs = dataDir + "qs" + batch + "-copyright-status.csv"
    with open(filenameQs, "w") as w, open(filenameCs, "w") as wc:
            for row in nonLicensed.itertuples():
                if type(row.start) != str and type(row.end) != str:
                    w.write(f'{row.qid}|P275|{row.licenseQID}|S248|Q5188229|S854|"https://api.crossref.org/v1/works/{row.doi}"\n')
                elif type(row.start) == str and type(row.end) != str:
                    w.write(f'{row.qid}|P275|{row.licenseQID}|P580|+{row.start}/11|S248|Q5188229|S854|"https://api.crossref.org/v1/works/{row.doi}"\n')
                elif type(row.start) == str and type(row.end) == str:
                    w.write(f'{row.qid}|P275|{row.licenseQID}|P580|+{row.start}/11|P582|+{row.end}/11|S248|Q5188229|S854|"https://api.crossref.org/v1/works/{row.doi}"\n')

                if row.licenseQID in ["Q6938433", "Q114756497"]: # public domain or cc zero
                    wc.write(f'{row.qid}|P6216|Q88088423\n')
                else:
                    wc.write(f'{row.qid}|P6216|Q50423863\n')


def licenseToCC(url):
    url = url.strip().lower().replace("http://", "https://")
    if url[-1:] == "/":
        w = url
        wo = url[:-1]
    else:
        w = url + "/"
        wo = url
    if w in licenses:
        return licenses[w]
    elif wo in licenses:
        return licenses[wo]
    else:
        with open(dataDir + "no-qid-for-license.txt", "a") as a:
            a.write(url + "\n")
        return None

def getStartEnd(license):
    if "start" in license:
        start = license["start"]["date-time"]
    else:
        start = None
        
    if "end" in license:
        end = license["start"]["date-time"]
    else:
        end = None

    return start, end

def defineArgParser():
    """Creates parser for command line arguments"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "-b",
        "--batch",
        action="store",
    )

    return parser


if __name__ == "__main__":

    argParser = defineArgParser()
    clArgs = argParser.parse_args()

    tick = time.time()
    # main(batch=clArgs.batch)
    # getIDs(batch=clArgs.batch)
    collate(batch=clArgs.batch)
    print(f'Elapsed time: {time.time() - tick} seconds')
