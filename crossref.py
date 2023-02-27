import argparse
import csv
import gzip
import json
import os
import re
import time
from collections import Counter

import pandas as pd
import pywikibot
from tqdm.rich import tqdm

import id_to_qid

dataDir = "crossref/data/"

with open(dataDir + "cc.json", "r") as r:
    licenses = json.load(r)


def main(batch):
    directory = dataDir + "April 2022 Public Data File from Crossref/"

    output = []
    # it = 0

    for filename in tqdm(os.listdir(directory)):
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
                        if "creativecommons" in k["URL"] or "gov" in k["URL"]:
                            qid = licenseToCC(k["URL"])
                            output.append([i["DOI"], k["URL"], qid])

        # it += 1
        # if it > 25:
        #     break

    if not batch:
        filename = dataDir + "CC.csv"
    else:
        filename = dataDir + "CC-" + batch + ".csv"
    with open(filename, "w", newline="") as w:
        w.write("doi,licenseURL,licenseQID\n")
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

    id_to_qid.main(inputList=dois, batch=batch, outputFileDir=dataDir + "qid-doi")


def collate(batch):
    if not batch:
        df1 = pd.read_csv(dataDir + "CC.csv")
        df1["doi"] = df1["doi"].str.upper()
        df2 = pd.read_csv(dataDir + "qid-doi.csv")
    else:
        df1 = pd.read_csv(dataDir + f"CC-{batch}.csv")
        df1["doi"] = df1["doi"].str.upper()
        df2 = pd.read_csv(dataDir + f"qid-doi-{batch}.csv")
    result = pd.merge(df1, df2, on="doi")

    # print(len(result))
    # exit()
    alreadyLicensed = result.loc[result["wdLicenseQID"].notnull()]
    alreadyLicensed.to_csv(dataDir + "alreadyLicensed.csv", index=False)

    nonLicensed = result.loc[
        result["wdLicenseQID"].isnull() & result["licenseQID"].notnull()
    ]

    filenameQs = dataDir + "qs.csv"
    if batch:
        filenameQs = dataDir + "qs" + batch + ".csv"
    with open(filenameQs, "w") as w:
        for row in nonLicensed.itertuples():

            # if row.licenseQID in ["Q6938433", "Q114756497"]:  # public domain or cc zero
            if type(row.start) != str and type(row.end) != str:
                w.write(f"{row.qid}|P275|{row.licenseQID}|P248|Q115868162\n")
            elif type(row.start) == str and type(row.end) != str:
                w.write(
                    f"{row.qid}|P275|{row.licenseQID}|P580|+{row.start}|P248|Q115868162\n"
                )
            elif type(row.start) == str and type(row.end) == str:
                w.write(
                    f"{row.qid}|P275|{row.licenseQID}|P580|+{row.start}|P582|+{row.end}|P248|Q115868162\n"
                )

            if row.licenseQID in ["Q6938433", "Q114756497"]:  # public domain or cc zero
                w.write(f"{row.qid}|P6216|Q88088423\n")
            else:
                w.write(f"{row.qid}|P6216|Q50423863\n")


def edit():
    input = "crossref/data/qs.csv"
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    t = tqdm(total=2887572, miniters=1)
    # i = 0
    with open(input, "r") as r:
        for i in range(12):
            next(r)
            t.update()
        for line in r:
            statement = line.split("|")

            # Lines are of the form
            # Q60431984|P275|Q20007257|P580|+2002-06-25T00:00:00Z/11|S248|Q115868162
            # Q60431984|P6216|Q50423863

            item = pywikibot.ItemPage(repo, statement[0])
            copyrightLicense = pywikibot.Claim(repo, statement[1])
            targetLicense = pywikibot.ItemPage(repo, statement[2])
            copyrightLicense.setTarget(targetLicense)
            if statement[1] == "P275":
                item.addClaim(copyrightLicense, summary='Add licence')
            else:
                item.addClaim(copyrightLicense, summary='Add licence status')

            if len(statement) > 3 and statement[3] == "P580":
                startTime = pywikibot.Claim(repo, statement[3])
                targetTime = pywikibot.WbTime.fromTimestr(statement[4], precision=11)
                startTime.setTarget(targetTime)
                copyrightLicense.addQualifier(startTime, summary="Add start time")

            if len(statement) > 5 and statement[5] == "P248":
                statedin = pywikibot.Claim(repo, statement[5])
                targetDataFile = pywikibot.ItemPage(repo, statement[6])
                statedin.setTarget(targetDataFile)
                copyrightLicense.addSources([statedin], summary="Add stated in")

            t.update()
            # i += 1
            # if i > 9:
            #     exit()


def licenseInfo():
    directory = dataDir + "April 2022 Public Data File from Crossref/"

    # Initialize a counter to store the frequencies of the URLs
    license_counts = Counter()
    # it = 0
    filenames = os.listdir(directory)
    start = 0
    end = 6702
    print(start, end)
    chunk = filenames[start:end]

    for filename in tqdm(chunk):
        f = os.path.join(directory, filename)

        with gzip.open(f, "r") as r:
            data = json.loads(r.read().decode("utf-8"))

        for o in data:
            for i in data[o]:
                if "license" in i:
                    for k in i["license"]:
                        # Increment the counter for the URL
                        license_counts[k["URL"]] += 1

        # it += 1
        # if it > 100:
        #     break

    # Write the results to a file in descending order
    with open(f"license_counts-{start}-{end}.txt", "w") as w:
        for license, count in sorted(
            license_counts.items(), key=lambda x: x[1], reverse=True
        ):
            w.write(f"{license}: {count}\n")


def collectLicenses():
    license_counts = Counter()

    license_files = [
        "license_counts-0-6702.txt",
        "license_counts-6703-13405.txt",
        "license_counts-13406-26810.txt",
    ]

    for l in license_files:
        with open(l, "r") as r:
            for line in r:
                match = re.search("^(.*): (\d+)$", line.strip())
                license_counts[match.group(1)] += int(match.group(2))

    with open(f"license_counts.txt", "w") as w:
        for license, count in sorted(
            license_counts.items(), key=lambda x: x[1], reverse=True
        ):
            w.write(f"{license}: {count}\n")


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
    # collate(batch=clArgs.batch)
    # licenseInfo()
    # collectLicenses()
    edit()
    print(f"Elapsed time: {time.time() - tick} seconds")
