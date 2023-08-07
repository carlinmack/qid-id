import gzip
import json
import os
import re
import time
from collections import Counter

import pandas as pd
import requests
from rdflib import Graph
from tqdm.rich import tqdm

import id_to_qid

data_dir = "crossref/coverage/"


def coverageInfo():
    directory = "crossref/data/ April 2022 Public Data File from Crossref/"
    filenames = os.listdir(directory)

    doisList = []
    prefixMap = {}
    publisherMap = {}

    doiIterator = 0
    focusTypes = [
        "journal-article",
        "book-chapter",
        "proceedings-article",
        "dissertation",
        "book",
        "monograph",
        "dataset",
    ]

    for filename in tqdm.tqdm(filenames):
        f = os.path.join(directory, filename)

        with gzip.open(f, "r") as r:
            data = json.loads(r.read().decode("utf-8"))

        for o in data:
            for i in data[o]:
                if "DOI" in i and i["type"] in focusTypes:
                    doisList.append(i["DOI"] + "\n")

                    if "container-title" in i:
                        prefixMap[i["prefix"]] = i["container-title"]
                        if "publisher" in i:
                            if i["publisher"] in publisherMap:
                                for ct in i["container-title"]:
                                    if ct not in publisherMap[i["publisher"]]:
                                        publisherMap[i["publisher"]].append(ct)
                            else:
                                publisherMap[i["publisher"]] = i["container-title"]

        if len(doisList) > 14000000:  # 14 million
            with open(data_dir + f"dois-{doiIterator}.csv", "w") as w:
                w.writelines(doisList)

            print(len(prefixMap), len(publisherMap))
            doisList = []
            doiIterator += 1

    with open(data_dir + "prefixs.json", "w", encoding="utf-8") as w:
        json.dump(prefixMap, w, ensure_ascii=False)
    with open(data_dir + "publishers.json", "w", encoding="utf-8") as w:
        json.dump(publisherMap, w, ensure_ascii=False)


def perPrefix():
    prefixes = Counter()
    pattern = re.compile("(^.+?)/")
    with open(data_dir + "dois-sorted.csv", "r") as r:
        for line in tqdm(r, total=112013354):
            match = re.search(pattern, line)
            prefixes[match.group(1)] += 1

    with open(data_dir + "prefix_counts.csv", "w") as w:
        for license, count in sorted(
            prefixes.items(), key=lambda x: x[1], reverse=True
        ):
            w.write(f"{license}: {count}\n")


def getIds():
    files = ["1-percent.txt"]
    for f in files:
        with open(data_dir + f) as r:
            dois = [l.strip() for l in r]

        id_to_qid.main(inputList=dois, outputFileDir=data_dir + "report/all")


def report():
    df = pd.read_csv(data_dir + "report/all.csv")
    alreadyLicensed = len(df.loc[df["wdLicenseQID"].notnull()])
    nonLicensed = df.loc[df["wdLicenseQID"].isnull()]

    all = 112013354
    sample = 1120133

    with open(data_dir + "report/all.txt", "w") as w:
        w.write(f"Total number of DOIs is {all:,}\n")
        w.write(f"Sample size was {sample:,} or {sample / all :.2%} of total\n")
        w.write(
            f"{len(df):,} were found on Wikidata, or {len(df) / sample:.2%} of the sample\n"
        )
        w.write(
            f"{alreadyLicensed:,} have a license in Wikidata, or {alreadyLicensed / len(df) :.2%} of those found\n"
        )


def doisFromWikidata():
    doisList = []
    doisIterator = 1
    pageIterator = 17502
    t = tqdm(total=286490, miniters=1)
    t.update(pageIterator)

    while True:
        url = f"https://query.wikidata.org/bigdata/ldf?predicate=http%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2FP356&page={pageIterator}"

        response = requests.get(
            url,
            headers={
                "User-Agent": "ID-to-QID <carlin.mackenzie@gmail.com>",
                "Accept": "text/turtle",
            },
        )

        try:
            graph = Graph()
            graph.parse(data=response.text)
        except:
            print(response.text)
            print(url)

        if len(graph) < 25:
            print("done")
            print(pageIterator)
            exit()

        for _, p, o in graph:
            if "wikidata" in p:
                doisList.append(o + "\n")

        t.update(1)
        pageIterator += 1

        if len(doisList) > 250000:  # 250k
            with open(data_dir + f"dois-from-wikidata-{doisIterator}.csv", "w") as w:
                w.writelines(doisList)

            doisList = []
            doisIterator += 1


def crossrefCoverage():
    with open(data_dir + "dois-from-wikidata-dedup.csv", "r") as r:
        wd = set(l.strip() for l in r)
        
    total_lines = 0
    covered_lines = 0
    t = tqdm(total=113000000, miniters=1)
    with open(data_dir + 'dois-sorted.csv', 'r') as r:
        for line in r:
            total_lines += 1
            t.update(1)
            if line.strip() in wd:
                covered_lines += 1
    coverage_ratio = covered_lines / len(wd)
    print('Coverage ratio: {:.2f}%'.format(coverage_ratio * 100))


if __name__ == "__main__":
    tick = time.time()
    # coverageInfo()
    # perPrefix()
    # getIds()
    # report()
    # doisFromWikidata()
    crossrefCoverage()
    print(f"Elapsed time: {time.time() - tick} seconds")
