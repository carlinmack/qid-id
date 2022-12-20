"""
If query.sparql and the input data file are downloaded, 
the script can simply be called as:

python iq-to-qid.py
"""

import argparse
import os
import time

import pandas as pd
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm, trange

HEADERS = {"User-Agent": "ID-to-QID"}


def main(
    test: bool = False,
    inputFile: str = "github.csv",
    pageSize: int = 125000,
    batchSize: int = 100,
    inputList=[],
    batch="",
):
    """From a list of IDs, go through and sequentially output a list of their properties and values."""

    with open("query.sparql") as r:
        query = r.readlines()
        query = "".join(query)

    if len(inputList) == 0:
        if not os.path.isfile(inputFile):
            print(inputFile + " does not exist")
            exit()

        with open(inputFile) as f:
            inputList = [line.strip() for line in f]

    for j in range(0, len(inputList), pageSize):
        if j < 4500000:
            continue
        start = j
        end = j + pageSize

        data = []

        for i in trange(start, end, batchSize):
            IDs = inputList[i : i + batchSize]

            if not IDs:
                break

            IDstring = " ".join(["'" + q + "'" for q in IDs])

            data += getData(query, IDstring)

            if test:
                break

        df = pd.DataFrame(data, columns=["qid", "doi", "wdLicenseQID"])
        if batch:
            df.to_csv("crossref/data/qid-doi-" + batch + ".csv", index=False)
        else:
            df.to_csv(
                "crossref/data/qid-doi-" + str(start) + "-" + str(end) + ".csv",
                index=False,
            )

        # missingDois = set(inputList).difference(set(df["doi"]))
        # with open("missing-dois.txt", "w") as w:
        #     w.write('\n'.join(missingDois))


def getData(query, IDstring):
    # print( query.format(
    #         values=IDstring,
    #     ))
    # exit()
    data = runQuery(
        query.format(
            values=IDstring,
        )
    )
    # print(query.format(
    #         values=IDstring,
    #     ))
    # exit()
    output = []

    for item in data["results"]["bindings"]:
        QID = item["item"]["value"][31:]
        id = item["id"]["value"]
        if "license" in item:
            license = item["license"]["value"]
        else:
            license = None
        output.append([QID, id, license])

    return output


def runQuery(query):
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
    main(test=clArgs.test)
    timer(tick)
