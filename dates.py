import csv
import gzip
import json
import os
import pickle
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import pandas as pd
from dateutil.parser import parse
from tqdm.rich import tqdm

import requests
from requests.exceptions import HTTPError
import id_to_qid


def date_index():
    directory = "crossref/data/April 2023 Public Data File from Crossref/"

    dates = {}

    it = 0
    lastit = 0

    for filename in tqdm(os.listdir(directory)):
        f = os.path.join(directory, filename)

        with gzip.open(f, "r") as r:
            data = json.loads(r.read().decode("utf-8"))

        for o in data:
            for i in data[o]:
                if "DOI" in i:
                    date = str(i["issued"]["date-parts"][0][0])
                    if len(i["issued"]["date-parts"][0]) == 2:
                        month = str(i["issued"]["date-parts"][0][1])
                        if len(month) == 1:
                            month = "0" + month
                        date += "-" + month
                    elif len(i["issued"]["date-parts"][0]) == 3:
                        month = str(i["issued"]["date-parts"][0][1])
                        if len(month) == 1:
                            month = "0" + month
                        day = str(i["issued"]["date-parts"][0][1])
                        if len(day) == 1:
                            day = "0" + day
                        date += "-" + month
                        date += "-" + day

                    dates[i["DOI"]] = date

        if it > 5000:
            with open(f"dates/dates-{lastit}.pickle", "wb") as w:
                pickle.dump(dates, w)
            lastit += it
            it = -1
            dates = {}

        it += 1

    with open(f"dates/dates-{lastit}.pickle", "wb") as w:
        pickle.dump(dates, w)

    # with open('dates.csv', 'w', newline='') as w:
    #     w.write("doi,date\n")
    #     for doi, date in dates.items():
    #         w.write(f"{doi},{date}\n")


def getIDs():
    # df = pd.read_csv("dates.json")
    # dois = df.iloc[:, 0]
    # dois = dois.values.tolist()
    with open("dates/dates-extremes.json", "r") as r:
        data = json.load(r)
    dois = list(data.keys())
    id_to_qid.main(inputList=dois, outputFileDir="dates-extremes")


def collate():
    def full_match(row):
        if type(row["wdDate"]) != str:
            return False
        if row["wdDate"][:10] == row["date"]:
            return True
        else:
            return False

    def year_match(row):
        if type(row["wdDate"]) != str:
            return False
        if row["wdDate"][:4] == row["date"][:4]:
            return True
        else:
            return False

    def difference(row):
        if type(row["wdDate"]) != str:
            return
        if row["date"] == "":
            return
        return parse(row["wdDate"][:10]) - parse(row["date"])

    def fix_date(row):
        if row["date"] == "None":
            return ""
        if len(row["date"]) == 10:
            return row["date"]
        elif len(row["date"]) == 7:
            return row["date"] + "-01"
        elif len(row["date"]) == 4:
            return row["date"] + "-01-01"

    def fix_wd_date(row):
        if type(row["wdDate"]) == str and "http" in row["wdDate"]:
            return
        return row["wdDate"]

    with open("dates/dates-extremes.json", "r") as r:
        data = json.load(r)
    input_data = [[k, v] for k, v in data.items()]
    df1 = pd.DataFrame(input_data, columns=["doi", "date"])
    df1["doi"] = df1["doi"].str.upper()
    df2 = pd.read_csv("dates-extremes-wd.csv")
    result = pd.merge(df1, df2, on="doi")

    result.rename(columns={"wdLicenseQID": "wdDate"}, inplace=True)
    result[result["wdDate"].isnull()][["date", "wdDate", "doi", "qid"]].to_csv(
        "dates-collated-no-wd.csv", index=False
    )

    result["date"] = result.apply(lambda row: fix_date(row), axis=1)
    result["wdDate"] = result.apply(lambda row: fix_wd_date(row), axis=1)
    result["date_match"] = result.apply(lambda row: full_match(row), axis=1)
    result["year_month_match"] = result.apply(lambda row: year_match(row), axis=1)
    result["difference"] = result.apply(lambda row: difference(row), axis=1)
    result.sort_values("difference", inplace=True)
    print(max(result["date"]))
    print(min(result["date"]))

    result[result["year_month_match"] == False][
        ["year_month_match", "date", "wdDate", "difference", "doi", "qid"]
    ].to_csv("dates-collated.csv", index=False)


def visualise():
    d = {}
    if False:
        files = [
            "dates-0.pickle",
            "dates-5001.pickle",
            "dates-10002.pickle",
            "dates-15003.pickle",
            "dates-20004.pickle",
            "dates-25005.pickle",
        ]

        for f in files:
            with open("dates/" + f, "rb") as r:
                data = pickle.load(r)

            for v in data.values():
                if v == "None":
                    continue
                val = int(v[:4])
                if val in d:
                    d[val] += 1
                else:
                    d[val] = 1

        with open("dates/years.csv", "w") as w:
            w.write("year,count\n")
            for k, v in d.items():
                w.write(f"{k},{v}\n")

        df = pd.read_csv("dates/years.csv")
        df.sort_values("year", inplace=True)
        df.to_csv("dates/years.csv", index=False)
    else:
        with open("dates/years.csv", newline="") as f:
            next(f)
            reader = csv.reader(f)
            for row in reader:
                d[int(row[0])] = int(row[1])
    fig, ax = plt.subplots()

    years = list(d.keys())
    ax.bar(years, list(d.values()))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylabel("Number of papers")
    ax.set_xlabel("Publication year")

    left = min(years) - 5
    right = max(years) + 5
    ax.set_xlim(left=left, right=right)

    ax.yaxis.set_major_formatter(tkr.FuncFormatter(lambda x, _: format(int(x), ",")))

    plt.gcf().set_size_inches(15, 6)

    plt.savefig(
        "years.png",
        bbox_inches="tight",
        pad_inches=0.25,
        dpi=400,
        format="png",
    )

    ax.set_yscale("log")
    ax.set_ylabel("Number of papers (log)")

    plt.savefig(
        "years-log.png",
        bbox_inches="tight",
        pad_inches=0.25,
        dpi=400,
        format="png",
    )

    plt.close(fig)


def get_extreme():
    output = {}
    files = [
        "dates-0.pickle",
        "dates-5001.pickle",
        "dates-10002.pickle",
        "dates-15003.pickle",
        "dates-20004.pickle",
        "dates-25005.pickle",
    ]

    for f in files:
        with open("dates/" + f, "rb") as r:
            data = pickle.load(r)

        for k, v in data.items():
            if v == "None":
                continue
            val = int(v[:4])
            if val < 1900 or val > 2023:
                output[k] = v

    with open("dates/dates-extremes.json", "w", encoding="utf-8") as w:
        json.dump(output, w, ensure_ascii=False)


def get_without_date():
    def run_query(it):
        output = []
        with open("dates/no-date.sparql") as r:
            query = r.readlines()
        
        query = "".join(query)
        query = query.format(limit = 100000 + it)

        data = runQuery(query)

        for o in data["results"]["bindings"]:
            output.append(o["item"]["value"][31:])

        return output

    url = "https://www.wikidata.org/w/api.php"

    output = set()
    it = 0
    for _ in range(100):
        output.update(run_query(it))
        print(len(output))
        time.sleep(1)
        it += 3000

    with open("dates/no-date-wd.txt", "a") as a:
        for d in output:
            a.write(d + "\n")


def runQuery(query):
    url = "https://query.wikidata.org/sparql"
    params = {"query": query, "format": "json"}
    try:
        response = requests.get(url, params=params, headers={"User-Agent": "User:Carlinmack"})
        return response.json()
    except HTTPError as e:
        print(response.text)
        print(e.response.text)
        print(query)
        return {"results": {"bindings": []}}
    except BaseException as err:
        print(query)
        print(f"Unexpected {err}\n")
        print(f"{type(err)}\n")
        raise


def get_dois():
    with open("dates/no-date-wd.txt", "r") as r:
        dois = r.read().splitlines()

    id_to_qid.main(inputList=dois, outputFileDir="dates/no-date-wd")


def find_dois():
    with open("file.txt", "r") as r:
        data = r.read().splitlines()
        dois = {i.split(",")[1]: i.split(",")[0] for i in data}

    output = []
    files = [
        "dates-0.pickle",
        "dates-5001.pickle",
        "dates-10002.pickle",
        "dates-15003.pickle",
        "dates-20004.pickle",
        "dates-25005.pickle",
    ]

    for f in files:
        with open("dates/" + f, "rb") as r:
            data = pickle.load(r)

        for k, v in data.items():
            if k in dois:
                date = qs_date({"date": v})
                output += [dois[k], date]

    with open("dates/no-dates-qs.txt", "w") as w:
        for row in output:
            w.write(f"{row[0]}|P577|{row[1]}|S248|Q118680719\n")


def create_qs():

    df = pd.read_csv(
        "dates/dates-collated-no-wd.csv",
        header=1,
        names=["date", "wdDate", "doi", "qid"],
    )
    df["qsDate"] = df.apply(lambda row: qs_date(row), axis=1)

    with open("dates/dates-qs.txt", "w") as w:
        for row in df.itertuples():
            w.write(f"{row.qid}|P577|{row.qsDate}|S248|Q118680719\n")


def qs_date(row):
    # +1839-00-00T00:00:00Z/9
    #  9 - year (default), 10 - month, 11 - day
    if len(row["date"]) == 4:
        return f"+{row['date']}-00-00T00:00:00Z/9"
    if len(row["date"]) == 7:
        return f"+{row['date']}-00T00:00:00Z/10"
    if len(row["date"]) == 10:
        return f"+{row['date']}T00:00:00Z/11"


if __name__ == "__main__":
    tick = time.time()
    # date_index()
    # getIDs()
    # collate()
    # visualise()
    # get_extreme()
    # create_qs()

    get_without_date()
    # get_dois()

    print(f"Elapsed time: {time.time() - tick:.4f} seconds")


# def get_without_date():
#     def query(url, params, headers):
#         lastContinue = {}
#         # it = 0
#         while True:
#             # Clone original request
#             p = params.copy()
#             # Modify it with the values returned in the 'continue' section of the last result.
#             p.update(lastContinue)
#             # Call API
#             result = requests.get(url, params=p, headers=headers).json()
#             if "error" in result:
#                 raise BaseException(result["error"])
#             if "warnings" in result:
#                 print(result["warnings"])
#             if "query" in result:
#                 yield result["query"]["search"]
#             if "continue" not in result:
#                 break
#             lastContinue = result["continue"]
#             # time.sleep(1)
#             # if it > 10:
#             #     break
#             # it += 1

#     url = "https://www.wikidata.org/w/api.php"

#     params = {
#         "action": "query",
#         "format": "json",
#         "list": "search",
#         "indexpageids": 1,
#         "continue": "",
#         "formatversion": "2",
#         "srsearch": "haswbstatement:P356 -haswbstatement:P577 -haswbstatement:P698",
#         "srnamespace": "0",
#         "srlimit": "max",
#         "srinfo": "",
#         "srprop": "",
#     }
#     headers = {"User-Agent": "User:Carlinmack"}

#     output = []
#     t = tqdm(total=20, miniters=1)

#     try:
#         for result in query(url, params, headers):
#             t.update()
#             for d in result:
#                 output.append(d["title"])
#     except:
#         with open("dates/no-date-wd-error.txt", "w") as w:
#             for d in output:
#                 w.write(d + "\n")
#         raise

#     with open("dates/no-date-wd.txt", "a") as a:
#         for d in output:
#             a.write(d + "\n")