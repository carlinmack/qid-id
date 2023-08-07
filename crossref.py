import argparse
import csv
import gzip
import json
import os
import pickle
import re
import time
from collections import Counter
from difflib import SequenceMatcher

import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import pandas as pd
import pywikibot
from tqdm.rich import tqdm
from unidecode import unidecode

import id_to_qid

import datetime
import glob

data_dir = "crossref/data/"

with open(data_dir + "cc.json", "r") as r:
    licenses = json.load(r)


def main(batch):
    directory = data_dir + "April 2023 Public Data File from Crossref/"

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
            continue

        for o in data:
            for i in data[o]:
                if "license" in i:
                    if len(i["license"]) == 1:
                        k = i["license"][0]
                        if "creativecommons" in k["URL"] or "gov" in k["URL"]:
                            qid = licenseToCC(k["URL"])
                            start, end = getStartEnd(k)
                            output.append([i["DOI"], k["URL"], qid, start, end])
        # it += 1
        # if it > 25:
        #     break
    if not batch:
        filename = data_dir + "CC.csv"
    else:
        filename = data_dir + "CC-" + batch + ".csv"
    with open(filename, "w", newline="") as w:
        w.write("doi,licenseURL,licenseQID,start,end\n")
        writer = csv.writer(w)
        writer.writerows(output)

    # df = pd.DataFrame(output)
    # licences = df.iloc[:, 1]
    # licences.drop_duplicates(inplace=True)
    # licences.to_csv(data_dir + "licenses.csv", index=False)


def getIDs(batch):
    if batch:
        file = data_dir + f"CC-{batch}.csv"
    else:
        file = data_dir + "CC.csv"

    df = pd.read_csv(file)
    dois = df.iloc[:, 0]
    dois = dois.values.tolist()

    id_to_qid.main(inputList=dois, batch=batch, outputFileDir=data_dir + "qid-doi")


def collate(batch):
    if not batch:
        df1 = pd.read_csv(data_dir + "CC.csv")
        df1["doi"] = df1["doi"].str.upper()
        df2 = pd.read_csv(data_dir + "qid-doi.csv")
    else:
        df1 = pd.read_csv(data_dir + f"CC-{batch}.csv")
        df1["doi"] = df1["doi"].str.upper()
        df2 = pd.read_csv(data_dir + f"qid-doi-{batch}.csv")
    result = pd.merge(df1, df2, on="doi")

    # print(len(result))
    # exit()
    alreadyLicensed = result.loc[result["wdLicenseQID"].notnull()]
    alreadyLicensed.to_csv(data_dir + "alreadyLicensed.csv", index=False)

    nonLicensed = result.loc[
        result["wdLicenseQID"].isnull() & result["licenseQID"].notnull()
    ]
    # print(nonLicensed.info())

    filenameQs = data_dir + "qs.csv"
    if batch:
        filenameQs = data_dir + "qs" + batch + ".csv"
    with open(filenameQs, "w") as w:
        for row in nonLicensed.itertuples():
            # print(row)
            # exit()
            # if row.licenseQID in ["Q6938433", "Q114756497"]:  # public domain or cc zero
            if type(row.start) != str and type(row.end) != str:
                w.write(f"{row.qid}|P275|{row.licenseQID}|P248|Q118680719\n")
            elif type(row.start) == str and type(row.end) != str:
                w.write(
                    f"{row.qid}|P275|{row.licenseQID}|P580|+{row.start}|P248|Q118680719\n"
                )
            elif type(row.start) == str and type(row.end) == str:
                w.write(
                    f"{row.qid}|P275|{row.licenseQID}|P580|+{row.start}|P582|+{row.end}|P248|Q118680719\n"
                )

            if row.licenseQID in ["Q6938433", "Q114756497"]:  # public domain or cc zero
                w.write(f"{row.qid}|P6216|Q88088423\n")
            else:
                w.write(f"{row.qid}|P6216|Q50423863\n")


def edit(skip=0):  # how many lines in the file to skip, used for running after a crash
    input = "crossref/data/qs.csv"
    site = pywikibot.Site("wikidata", "wikidata")
    site.login()
    repo = site.data_repository()

    t = tqdm(total=2887572, miniters=1)
    # i = 0
    with open(input, "r") as r:
        for _ in range(skip):
            next(r)
            t.update()
        for line in r:
            statement = line.split("|")

            # Q60431984|P275|Q20007257|P580|+2002-06-25T00:00:00Z/11|S248|Q118680719
            # Q60431984|P6216|Q50423863
            # print(statement)
            # exit()
            try:
                item = pywikibot.ItemPage(repo, statement[0])
                copyrightLicense = pywikibot.Claim(repo, statement[1])
                targetLicense = pywikibot.ItemPage(repo, statement[2])
                copyrightLicense.setTarget(targetLicense)
                if statement[1] == "P275":
                    item.addClaim(copyrightLicense, summary="Add licence")
                else:
                    item.addClaim(copyrightLicense, summary="Add licence status")

                if len(statement) > 3 and statement[3] == "P580":
                    startTime = pywikibot.Claim(repo, statement[3])
                    targetTime = pywikibot.WbTime.fromTimestr(
                        statement[4], precision=11
                    )
                    startTime.setTarget(targetTime)
                    copyrightLicense.addQualifier(startTime, summary="Add start time")

                    if len(statement) > 5 and statement[5] == "P248":
                        statedin = pywikibot.Claim(repo, statement[5])
                        targetDataFile = pywikibot.ItemPage(repo, statement[6])
                        statedin.setTarget(targetDataFile)
                        copyrightLicense.addSources([statedin], summary="Add stated in")
                elif len(statement) > 3 and statement[3] == "P248":
                    statedin = pywikibot.Claim(repo, statement[3])
                    targetDataFile = pywikibot.ItemPage(repo, statement[4])
                    statedin.setTarget(targetDataFile)
                    copyrightLicense.addSources([statedin], summary="Add stated in")
                # exit()
                t.update()
                # i += 1
                # if i > 5:
                #     exit()
            except Exception as e:
                with open("failedQS.csv", "a") as a:
                    a.write(line)
                with open("failedQS-reasons.csv", "a") as a:
                    a.write(str(e))
                time.sleep(10)


def search(batch):
    directory = data_dir + "April 2023 Public Data File from Crossref/"

    output = []
    # it = 0

    taxons = [
        "aquatic invasion",
        "biological invasion",
        "biotic invasion pattern",
        "coastal invasion",
        "ecological invasion risk",
        "estuarine invasion",
        "invasion biology",
        "invasion ecology",
        "invasion facilitation",
        "invasion genetics",
        "invasion impact",
        "invasion management",
        "invasion pathway",
        "invasion success",
        "invasive ant",
        "invasive aspergillosis",
        "invasive candidiasis",
        "invasive fish",
        "invasive grass",
        "invasive mosquito species",
        "invasive plant",
        "invasive shrub",
        "invasive species",
        "invasive species management",
        "invasive trait",
        "invasive tree",
        "invasive weed",
        "plant invasion",
        "urban invasion",
    ]

    taxons = [s.casefold() for s in taxons]

    for filename in tqdm(os.listdir(directory)):
        f = os.path.join(directory, filename)

        try:
            with gzip.open(f, "r") as r:
                data = json.loads(r.read().decode("utf-8"))
        except:
            print("😳")
            print(f)
            continue

        for o in data:
            for i in data[o]:
                if all(
                    x in i for x in ["license", "title", "abstract", "subject", "DOI"]
                ):
                    # if len(i["license"]) == 1:
                    k = i["license"][0]

                    title = i["title"][0].casefold()
                    abstract = i["abstract"].casefold()
                    if "inv" in title or "inv" in abstract:
                        for s in taxons:
                            if s in title or s in abstract:
                                qid = licenseToCC(k["URL"])
                                output.append(
                                    [i["DOI"], k["URL"], qid, s, i["subject"]]
                                )
        # it += 1
        # if it > 5:
        #     break
    if not batch:
        filename = data_dir + "results.csv"
    else:
        filename = data_dir + "results-" + batch + ".csv"
    with open(filename, "w", newline="") as w:
        w.write("doi\tlicenseURL\tlicenseQID\tmatch\tsubjects\n")
        writer = csv.writer(w, delimiter="\t")
        writer.writerows(output)

    # df = pd.DataFrame(output)
    # licences = df.iloc[:, 1]
    # licences.drop_duplicates(inplace=True)
    # licences.to_csv(data_dir + "licenses.csv", index=False)


def search_taxon(batch):
    def matches(input_string, target_phrases: set, skip_words: set) -> str:
        words = input_string.split()
        i = 0

        while i < len(words) - 1:
            first_word = words[i]
            second_word = words[i + 1]
            phrase = f"{first_word} {second_word}"

            if second_word in skip_words:
                i += 2  # Skip the current pair and move to the next one
            else:
                if phrase in target_phrases:
                    return phrase
                else:
                    i += 1  # Move to the next pair

        return ""

    def output_json(iterator: int, data: dict) -> None:
        with open(
            f"{data_dir}/results-taxon-{iterator}.json", "w", encoding="utf-8"
        ) as w:
            json.dump(data, w, ensure_ascii=False)

    def extract_keys(data: dict):
        keys_to_extract = [
            "license",
            "title",
            "abstract",
            "subject",
            "issued",
            "author",
            "publisher",
            "container-title",
            "ISSN",
            "resource",
        ]
        result = {key: data[key] for key in keys_to_extract if key in data}
        return result

    directory = data_dir + "April 2023 Public Data File from Crossref/"

    output = {}
    it = 0
    count = 0

    with open("crossref/data/taxons-sorted.csv", "r") as r:
        taxons = r.read().splitlines()

    taxons = set([s.casefold() for s in taxons])

    with open("crossref/data/stopwords.txt", "r") as r:
        stopwords = r.read().splitlines()

    stopwords = set([s.casefold() for s in stopwords])

    # for a in stopwords:
    #     for b in taxons:
    #         if re.search(r"\b" + a + r"\b", b):
    #             print(a, b)
    # exit()

    for filename in tqdm(os.listdir(directory)):
        f = os.path.join(directory, filename)

        try:
            with gzip.open(f, "r") as r:
                data = json.loads(r.read().decode("utf-8"))
        except:
            print("😳")
            print(f)
            continue

        for o in data:
            for i in data[o]:
                if all(x in i for x in ["license", "title", "abstract", "DOI"]):
                    count += 1
                    k = i["license"][0]

                    title = i["title"][0].casefold()
                    abstract = i["abstract"].casefold()

                    qid = licenseToCC(k["URL"])
                    m = matches(title, taxons, stopwords) or matches(
                        abstract, taxons, stopwords
                    )
                    if m:
                        output[i["DOI"]] = extract_keys(i)
                        output[i["DOI"]]["match"] = m
                        output[i["DOI"]]["licenseQID"] = qid
        # it += 1
        # if it > 5:
        #     break

        # Output the data as JSON every 10k ORCIDs
        if len(output) >= 10000:
            output_json(it, output)
            it += 1
            output = {}

    # Output any remaining data
    if output:
        output_json(it, output)

    print(f"items: {count}")

    # df = pd.DataFrame(output)
    # licences = df.iloc[:, 1]
    # licences.drop_duplicates(inplace=True)
    # licences.to_csv(data_dir + "licenses.csv", index=False)


def annotate_matches():
    def matches(input_string, target_phrases: set) -> set:
        words = input_string.split()
        i = 0
        matches = set()

        while i < len(words) - 1:
            first_word = words[i]
            second_word = words[i + 1]
            phrase = f"{first_word} {second_word}"

            if phrase in target_phrases:
                matches.add(phrase)

            i += 1  # Move to the next pair

        return matches

    if True:
        with open(f"{data_dir}/results-taxon.pickle", "rb") as r:
            data = pickle.load(r)

        with open(f"{data_dir}/taxons-sorted.csv", "r") as r:
            taxons = r.read().splitlines()

        with open(f"{data_dir}/taxons-qid.csv", 'r') as r:
            qids = {i.split(",")[1].casefold(): i.split(",")[0] for i in r.read().splitlines()}

        taxons = set([s.casefold() for s in taxons])

        for doi, d in data.items():
            title = d["title"][0].casefold()
            abstract = d["abstract"].casefold()

            new_matches = list(matches(title, taxons).union(matches(abstract, taxons)))

            # assert d["match"] in new_matches
            matches_qids = [qids[m] for m in new_matches]
            data[doi]["matches"] = new_matches
            data[doi]["matches-qid"] = matches_qids

        with open("test.json", "w", encoding="utf-8") as w:
            json.dump(data["10.15294/biosaintifika.v10i2.12934"], w, ensure_ascii=False)

        with open(f"{data_dir}/results-taxon-annotated.pickle", "wb") as w:
            pickle.dump(data, w)
    else:
        pass

def filter():
    def getDate(input):
        if len(input) == 1:
            return f"+{input[0]}-00-00T00:00:00Z/9"
        if len(input) == 2:
            return f"+{input[0]}-{input[1]:02d}-00T00:00:00Z/10"
        if len(input) == 3:
            return f"+{input[0]}-{input[1]:02d}-{input[2]:02d}T00:00:00Z/11"

    with open(f"{data_dir}/results-taxon-annotated.pickle", "rb") as r:
        data = pickle.load(r)

    output = []
    for doi, d in data.items():
        year = d["issued"]["date-parts"][0][0]
        if year > 2015:
            date = getDate(d["issued"]["date-parts"][0])
            output += {
                "doi": doi,
                "date": date, 
                "licenseQID": d["licenseQID"], 
                "matchesQIDs": d["matches-qid"],
                "resource": d["resource"]
            }

def in_wikidata():
    if False:
        dois = []
        with open(f"{data_dir}/results-taxon.pickle", "rb") as r:
            data = pickle.load(r)

        for k in data.keys():
            dois.append(k.upper())

        with open(f"{data_dir}/results-taxon-dois.txt", "w") as w:
            for d in dois:
                w.write(d + "\n")
    else:
        with open(f"{data_dir}/results-taxon-dois.txt", "r") as r:
            dois = r.read().splitlines()

        id_to_qid.main(inputList=dois, outputFileDir=data_dir + "results-taxon-dois")


def plot_years():
    def fix_date(date):
        if date == "None" or type(date) != str:
            return ""
        if len(date) == 7:
            return date + "-01"
        if len(date) == 4:
            return date + "-01-01"
        return date

    if False:
        if False:
            all = glob.glob(f"{data_dir}/results-taxon-*.json")
            output = {}
            for filename in all:
                with open(filename, "r") as r:
                    data = json.load(r)

                output = output | data

            with open(f"{data_dir}/results-taxon.pickle", "wb") as w:
                pickle.dump(output, w)
        else:
            with open(f"{data_dir}/results-taxon.pickle", "rb") as r:
                data = pickle.load(r)

        if True:
            dates = {}
            for doi, i in data.items():
                if "issued" in i:
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

                    dates[doi] = date

            with open(f"{data_dir}/results-taxon-dates.pickle", "wb") as w:
                pickle.dump(dates, w)
    else:
        with open(f"{data_dir}/results-taxon-dates.pickle", "rb") as r:
            dates = pickle.load(r)

        print(len(dates))

        df_wikidata = pd.read_csv(f"{data_dir}/results-taxon-dois-all.csv")
        df_wikidata.rename(columns={"wdLicenseQID": "wdDate"}, inplace=True)
        print(len(df_wikidata))

    df = pd.DataFrame.from_dict(dates, orient="index").reset_index()
    df.rename(columns={"index": "doi", 0: "date"}, inplace=True)

    df["doi"] = df["doi"].str.upper()
    result = pd.merge(df, df_wikidata, on="doi", how="outer")

    result["date"] = result["date"].apply(fix_date)
    result["date"] = pd.to_datetime(result["date"])
    result["year"] = result["date"].dt.year

    if False:  ## visualise
        # print(result)
        filtered_df = result[result["qid"].notnull()]
        print(len(result), len(filtered_df))

        grouped = result.groupby("year").size()
        filtered_grouped = filtered_df.groupby("year").size()
        _, ax = plt.subplots()

        years = list(grouped.index)
        # ax.bar(not_fixed_grouped.index, not_fixed_grouped.values, label="Not fixed")
        ax.bar(grouped.index, grouped.values, label="All")
        ax.bar(filtered_grouped.index, filtered_grouped.values, label="In Wikidata")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_ylabel("Number of papers")
        ax.set_xlabel("Publication year")
        # ax.set_title("Papers returned from 29 invasion related search terms by year")
        ax.set_title("Publication date of taxon papers")
        ax.legend()

        # left = min(years) - 1
        left = 1970
        right = max(years) + 1
        ax.set_xlim(left=left, right=right)

        ax.yaxis.set_major_formatter(
            tkr.FuncFormatter(lambda x, _: format(int(x), ","))
        )

        plt.gcf().set_size_inches(11, 6)

        plt.savefig(
            f"{data_dir}/years-api.png",
            bbox_inches="tight",
            pad_inches=0.25,
            dpi=400,
            format="png",
        )

        ax.set_yscale("log")
        ax.set_ylabel("Number of papers (log)")

        ax.yaxis.set_major_formatter(
            tkr.FuncFormatter(lambda x, _: format(int(x), ","))
        )

        plt.savefig(
            f"{data_dir}/years-api-log.png",
            bbox_inches="tight",
            pad_inches=0.25,
            dpi=400,
            format="png",
        )

    if True:  ## output DOIs
        result[result["qid"].isna()]["doi"].to_csv(
            f"{data_dir}taxon-dois-not-in-wikidata.csv", index=False
        )


def licenseInfo():
    directory = data_dir + "April 2022 Public Data File from Crossref/"

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


def orcids():
    directory = data_dir + "April 2023 Public Data File from Crossref/"

    # Initialize a counter to store the frequencies of the URLs
    authors = {}
    # orcid: {"name str": [dois]}
    it = 28001
    lastit = 28001

    for filename in tqdm(os.listdir(directory)[28000:]):
        f = os.path.join(directory, filename)

        with gzip.open(f, "r") as r:
            data = json.loads(r.read().decode("utf-8"))

        for o in data:
            for i in data[o]:
                if "author" in i:
                    for k in i["author"]:
                        if "ORCID" in k:
                            given = k.get("given", "")
                            family = k.get("family", "")
                            name = given + " " + family
                            orcid = k["ORCID"]
                            doi = i["DOI"]

                            if orcid in authors:
                                if name in authors[orcid]:
                                    authors[orcid][name].append(doi)
                                else:
                                    authors[orcid][name] = [doi]
                            else:
                                authors[orcid] = {name: [doi]}

        if it % 3500 == 0:
            pass
            # with open(f"authors-{lastit}-{it}.json", "w", encoding="utf-8") as w:
            #     json.dump(authors, w, ensure_ascii=False)

        it += 1

    with open(f"authors/authors-{lastit}-{it}.pickle", "wb") as w:
        pickle.dump(authors, w)
    authors = {}
    lastit = it


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

    with open("license_counts.txt", "w") as w:
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
        with open(data_dir + "no-qid-for-license.txt", "a") as a:
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

    parser.add_argument("-s", "--skip", type=int)

    return parser


if __name__ == "__main__":

    argParser = defineArgParser()
    clArgs = argParser.parse_args()

    tick = time.time()
    # main(batch=clArgs.batch)
    # getIDs(batch=clArgs.batch)
    # collate(batch=clArgs.batch)

    # edit(skip=clArgs.skip)

    # licenseInfo()
    # collectLicenses()

    # orcids()
    # consolidate_orcids()
    # find_duplicate_orcids()
    # orcid_report()

    # transform_authors()

    # search(batch=clArgs.batch)

    # search_taxon(batch=clArgs.batch)
    # annotate_matches()
    filter()
    # in_wikidata()
    # plot_years()
    print(f"Elapsed time: {time.time() - tick:.5f} seconds")
