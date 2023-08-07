import csv
import json
import os
import pickle
import time

import pandas as pd
from tqdm.rich import tqdm

import id_to_qid

data_dir = "doaj/"
journalDir = data_dir + "doaj_journal_data_2022-12-19/"
articleDir = data_dir + "doaj_article_data_2022-12-19/"


with open(data_dir + "cc.json", "r") as r:
    licenses = json.load(r)


def getArticles():
    with open(data_dir + "journalData.pickle", "rb") as r:
        journalData = pickle.load(r)

    output = []
    for filename in tqdm(os.listdir(articleDir)):
        f = os.path.join(articleDir, filename)
        with open(f, "r") as r:
            articlesRaw = r.read()

        articles = json.loads(articlesRaw)

        # it = 0
        for article in articles:
            # print(article)
            issn = article["bibjson"]["journal"]["issns"][0]
            if issn in journalData:
                if "year" in article["bibjson"] and len(article["bibjson"]["year"]) == 4:
                    if int(article["bibjson"]["year"]) > journalData[issn]["year"]:

                        for id in article["bibjson"]["identifier"]:
                            if id["type"] == "doi" and "id" in id:
                                output.append([id["id"], journalData[issn]["license"]])
            # exit()
        # print(".", end="")
        # it +=1
        # if it > 500:
        #     break
    filename = data_dir + "CC.csv"
    with open(filename, "w", newline="") as w:
        w.write("doi,licenseQID\n")
        writer = csv.writer(w)
        writer.writerows(output)


def getIDs():
    batch = ""
    if batch:
        file = data_dir + f"CC-{batch}.csv"
    else:
        file = data_dir + "CC.csv"

    df = pd.read_csv(file)
    dois = df.iloc[:, 0]
    dois = dois.values.tolist()
    print(len(dois))

    id_to_qid.main(inputList=dois, batch=batch)


def collate():
    df1 = pd.read_csv(data_dir + "CC.csv")
    df2 = pd.read_csv(data_dir + "qid-doi.csv")
    result = pd.merge(df1, df2, on="doi")

    alreadyLicensed = result.loc[result["wdLicenseQID"].notnull()]
    alreadyLicensed.to_csv(data_dir + "alreadyLicensed.csv", index=False)

    nonLicensed = result.loc[result["wdLicenseQID"].isnull()]

    filenameQs = data_dir + "qs.csv"

    with open(filenameQs, "w") as w:
        for row in nonLicensed.itertuples():
            w.write(
                f'{row.qid}|P275|{row.licenseQID}\n'
            )
            

            if row.licenseQID in ["Q6938433", "Q114756497"]:  # public domain or cc zero
                w.write(f"{row.qid}|P6216|Q88088423\n")
            else:
                w.write(f"{row.qid}|P6216|Q50423863\n")


def getJournalData():
    # Open the file and read the contents
    with open(journalDir + "journal.json", "r") as r:
        contents = r.read()

    # Parse the JSON data
    data = json.loads(contents)

    # Use a list comprehension to create a new list containing only objects with one license
    output_data = {
        **{
            item["bibjson"]["pissn"]: {
                "year": int(item["bibjson"]["oa_start"]),
                "license": urlToQid(item["bibjson"]["license"][0]["url"]),
            }
            for item in data
            if "pissn" in item["bibjson"]
        },
        **{
            item["bibjson"]["eissn"]: {
                "year": int(item["bibjson"]["oa_start"]),
                "license": urlToQid(item["bibjson"]["license"][0]["url"]),
            }
            for item in data
            if "eissn" in item["bibjson"]
        },
    }

    output = data_dir + "journalData.pickle"
    with open(output, "wb") as w:
        pickle.dump(output_data, w)


def urlToQid(url):
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
        with open(data_dir + "no-qid-for-license.txt", "a") as a:
            a.write(url + "\n")
        return None


def filterJournals():
    # Open the file and read the contents
    with open(journalDir + "journal_batch_1.json", "r") as r:
        contents = r.read()

    # Parse the JSON data
    data = json.loads(contents)

    # Use a list comprehension to create a new list containing only objects with one license
    filtered_data = [
        item
        for item in data
        if len(item["bibjson"]["license"]) == 1
        # and item["bibjson"]["license"][0]["type"] == "CC BY"
        and "url" in item["bibjson"]["license"][0]
        and item["bibjson"]["copyright"]["author_retains"] == False
        and "oa_start" in item["bibjson"]
    ]

    # for item in filtered_data:
    #     for identifier in filtered_data["bibjson"]["identifiers"]:
    #         item["bibjson"]["identifiers"][identifier["type"]] = identifier["id"]

    # Write the filtered data to a new JSON file
    with open(journalDir + "journal.json", "w") as w:
        json.dump(filtered_data, w)


if __name__ == "__main__":
    tick = time.time()
    # filterJournals()
    # getJournalData()
    # getArticles()
    # getIDs()
    collate()
    print(f"Elapsed time: {time.time() - tick} seconds")
