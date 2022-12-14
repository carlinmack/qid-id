import argparse
import html
import os
import time
import csv
import re
import pandas as pd
import requests
from requests.exceptions import HTTPError
from tqdm import trange

HEADERS = {"User-Agent": "ID-to-QID"}


def main(
    test: bool = False,
    inputFile: str = "QIDs.txt",
    pageSize: int = 125000,
    batchSize: int = 100,
):
    """From a list of IDs, go through and sequentially output a list of their properties and values."""

    if not os.path.isfile(inputFile):
        print(inputFile + " does not exist")
        exit()

    with open("title.sparql") as r:
        query = r.readlines()
        query = "".join(query)

    with open(inputFile) as f:
        inputList = [line.strip() for line in f]

    for j in range(3250000, len(inputList), pageSize):
        start = j
        end = j + pageSize

        data = []

        for i in trange(start, end, batchSize):
            IDs = inputList[i : i + batchSize]

            if not IDs:
                break

            IDstring = " ".join(["wd:" + q for q in IDs])

            data += getData(query, IDstring)

            if test:
                break

        df = pd.DataFrame(data, columns=["qid", "title"])
        df.to_csv("data/qid-title-" + str(start) + "-" + str(end) + ".csv", index=False)
        if test:
            break


def titleProcessing():
    df = pd.read_csv("qid-title.csv")

    # freq_df = df["title"].value_counts(ascending=False)
    # freq_df.head(1000).to_csv("qid-title-freq.csv")

    html_df = df[df.title.str.contains("<(sup|sub|i|SUP|SUB|I)>", regex=True, na=False)]
    html_df.to_csv("qid-title-html.csv", index=False)

    special_df = df[df.title.str.contains("&\w\w+;", regex=True, na=False)]
    special_df.to_csv("qid-title-special.csv", index=False)

    # Perform multiple times as strings can be encoded multiple times
    for _ in range(10):
        special_df["title"] = special_df["title"].apply(lambda x: html.unescape(x))

    # filter dataframes based on presence of html tags
    cond = special_df.title.str.contains(
        "<(sup|sub|i|SUP|SUB|I)>", regex=True, na=False
    )
    just_special_df = special_df[~cond]
    special_html_df = special_df[cond]

    both_html_df = pd.concat([html_df, special_html_df])

    just_special_df["title"] = just_special_df["title"].apply(lambda x: '"' + x + '"')
    both_html_df["title"] = both_html_df["title"].apply(lambda x: '"' + x + '"')

    just_special_df["P1476"] = "P1476"
    just_special_df = just_special_df[["qid", "P1476", "title"]]
    just_special_df.to_csv(
        "qsv1-special.tsv", sep="\t", index=False, header=False, quoting=csv.QUOTE_NONE
    )

    both_html_df["P1476"] = "P1476"
    both_html_df["qal6833"] = "qal6833"
    both_html_df["no_html"] = both_html_df.apply(lambda row: removeHTML(row), axis=1)
    both_html_df = both_html_df[["qid", "P1476", "no_html", "qal6833", "title"]]
    both_html_df.to_csv(
        "qsv1-html.tsv", sep="\t", index=False, header=False, quoting=csv.QUOTE_NONE
    )


def removeHTML(row):
    return re.sub(
        r"(<sup>|</sup>|<sub>|</sub>|<i>|</i>)", "", row["title"], flags=re.IGNORECASE
    )


def getData(query, IDstring):
    data = runQuery(
        query.format(
            values=IDstring,
        )
    )
    output = []

    for item in data["results"]["bindings"]:
        QID = item["item"]["value"][31:]
        id = item["title"]["value"]
        output.append([QID, id])

    return output


def runQuery(query):
    # print(query)
    url = "https://query.wikidata.org/sparql"
    params = {"query": query, "format": "json"}
    try:
        response = requests.get(url, params=params, headers=HEADERS)
        return response.json()
    except HTTPError as e:
        print(response.text)
        print(e.response.text)
        print(query)
        return {"results": {"bindings": []}}
    except BaseException as err:
        print(query)
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def timer(tick, msg=""):
    print("--- %s %.3f seconds ---" % (msg, time.time() - tick))
    return time.time()


def defineArgParser():
    """Creates parser for command line arguments"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "-t",
        "--test",
        help="",
        action="store_true",
    )

    return parser


if __name__ == "__main__":

    argParser = defineArgParser()
    clArgs = argParser.parse_args()

    tick = time.time()
    # main(test=clArgs.test)
    titleProcessing()
    timer(tick)
