import csv
import gzip
import json
import os
import pickle
import time
from collections import Counter
from difflib import SequenceMatcher

import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import pandas as pd
from tqdm.rich import tqdm
from unidecode import unidecode


def consolidate_orcids() -> None:
    author_files = [
        "authors-21000-24500.pickle",
        "authors-0-3500.pickle",
        "authors-3501-7000.pickle",
        "authors-7000-10500.pickle",
        "authors-10500-14000.pickle",
        "authors-14000-17500.pickle",
        "authors-17500-21000.pickle",
        "authors-24500-28000.pickle",
        "authors-28001-28702.pickle",
    ]
    with open("authors/" + author_files[0], "rb") as r:
        authors = pickle.load(r)

    for f in author_files[1:]:
        with open(f, "rb") as r:
            working_file = pickle.load(r)
        for key, value in working_file.items():
            if key in authors:
                for k, v in value.items():
                    if k in authors[key]:
                        authors[key][k] += v
                    else:
                        authors[key][k] = v
            else:
                authors[key] = working_file[key]

    with open("authors/authors.pickle", "wb") as w:
        pickle.dump(authors, w)
    # with open('authors.json', 'w', encoding='utf-8') as w:
    #     json.dump(authors, w, ensure_ascii=False)


def find_duplicate_orcids() -> None:
    def any_duplicate(dois):
        flat = [d for i in dois for d in i]
        return len(flat) != len(set(flat))

    # filenames = ["authors-0-7000","authors-7000-14000","authors-14000-21000"]
    # with open("authors-1-2.json", 'r') as r:
    #     data = json.load(r)

    if False:  ## open authors.pickle
        with open("authors/authors.pickle", "rb") as r:
            data = pickle.load(r)

        if False:  ## Filter authors
            filtered = {k: v for k, v in data.items() if len(v) == 1}

        if False:  ## output ORCIDs with > 1 author strings
            with open("authors/orcids-duplicates-all.txt", "w") as w:
                for d in data.keys():
                    w.write(d + "\n")

    if False:  ## output definite duplicates
        output = {k: v for k, v in filtered.items() if any_duplicate(list(v.values()))}

        with open(
            "authors/authors-duplicates-confirmed.json", "w", encoding="utf-8"
        ) as w:
            json.dump(output, w, ensure_ascii=False)

    if False:  ## annotated with ORCID API
        if False:  ## create authors-annotated.pickle
            with open("authors/orcid-api-all.pickle", "rb") as r:
                api = pickle.load(r)

            annotated = {}
            count = 0
            for k in data.keys():
                if k in api:
                    if (
                        len(api[k]["name"]) > 0
                        or len(api[k]["other_names"]) > 0
                        or len(api[k]["credit_name"]) > 0
                    ):
                        annotated[k] = {"data": data[k], "orcid": api[k]}
                    else:
                        count += 1

            print(f"ORCIDs without any name: {count}\n")

            with open("authors/authors-annotated-all.pickle", "wb") as w:
                pickle.dump(annotated, w)
        else:
            with open("authors/authors-annotated-all.pickle", "rb") as r:
                annotated = pickle.load(r)

            if False:  ## fix credit name 🤷
                for v in annotated.values():
                    if "credit_name" not in v["orcid"]:
                        v["orcid"]["credit_name"] = ""

                with open("authors/authors-annotated-all.pickle", "wb") as w:
                    pickle.dump(annotated, w)

        if False:  ## similarity with canonical
            output = {
                k: v
                for k, v in annotated.items()
                if min_similarity(
                    list(v["data"].keys()),
                    [
                        v["orcid"]["name"],
                        *v["orcid"]["other_names"],
                        v["orcid"]["credit_name"],
                    ],
                )
                < 0.2
            }

            if False:  ## one line
                for o, d in output.items():
                    for k, v in d["data"].items():
                        output[o]["data"][k] = v[0]

            with open(
                "authors/authors-annotated-duplicates-all.json",
                "w",
                encoding="utf-8",
            ) as w:
                json.dump(output, w, ensure_ascii=False)
        else:
            with open("authors/authors-annotated-duplicates-all.json", "r") as r:
                output = json.load(r)

        if False:  ## prefix report with ORCID
            dois = {"matched": set(), "unmatched": set()}
            unmatched_samples = {}
            for k, v in output.items():
                name_strings = list(v["data"].keys())
                canonical_strings = [
                    v["orcid"]["name"],
                    *v["orcid"]["other_names"],
                    v["orcid"]["credit_name"],
                ]
                canonical_strings = [c for c in canonical_strings if len(c) > 0]
                if len(canonical_strings) > 0:
                    for name in name_strings:
                        max_sim = 0
                        for canon in canonical_strings:
                            s = similarity(name, canon)

                            if s > max_sim:
                                max_sim = s

                        if max_sim < 0.2:
                            category = "unmatched"

                            for doi in v["data"][name]:
                                prefix = doi.split("/")[0]
                                if prefix not in unmatched_samples:
                                    unmatched_samples[prefix] = [name, canon, doi]

                            # dois[category].add(doi)
                        else:
                            category = "matched"

                        for doi in v["data"][name]:
                            dois[category].add(doi)

            counts = {}
            for category in ["matched", "unmatched"]:
                prefix_counts = Counter()
                for doi in dois[category]:
                    prefix = doi.split("/")[0]
                    prefix_counts[prefix] += 1

                df = pd.DataFrame.from_dict(prefix_counts, orient="index").reset_index()
                df.rename(columns={"index": "doi", 0: f"{category}"})
                counts[category] = df
                with open(
                    f"authors/authors-annotated-prefixes-all-{category}.txt", "w"
                ) as w:
                    for prefix, count in sorted(
                        prefix_counts.items(), key=lambda x: x[1], reverse=True
                    ):
                        if category == "matched":
                            w.write(f"{prefix}: {count:,}\n")
                        else:
                            w.write(f"{prefix}: {count:,}. ")
                            w.write(f"Sample: {unmatched_samples[prefix][0]}, ")
                            w.write(f"should be {unmatched_samples[prefix][1]} ")
                            w.write(f"for {unmatched_samples[prefix][2]}\n")

    if True: ## proportion
        with open("authors/authors-annotated-prefixes-all-matched.txt", "r") as r:
            matched_counts = [(i.split(": ")) for i in r.read().splitlines()]

        matched = pd.DataFrame.from_records(matched_counts, columns=["doi", "matched"])
        matched["matched"] = matched["matched"].str.replace(",", "").astype(int)

        with open("authors/authors-annotated-prefixes-all-unmatched.txt", "r") as r:
            unmatched_counts = [
                (i.split(". ", 1)[0].split(": ")) for i in r.read().splitlines()
            ]

        unmatched = pd.DataFrame.from_records(
            unmatched_counts, columns=["doi", "unmatched"]
        )
        unmatched["unmatched"] = (
            unmatched["unmatched"].str.replace(",", "").astype(int)
        )

        result = pd.merge(matched, unmatched, on="doi", how="outer")

        result.set_index('doi', inplace=True)
        result = result.fillna(0).astype(int)
        result["sum"] = result["unmatched"] + result["matched"]
        
        result = result[result["sum"] > 100]

        # min_sum = result['unmatched'].min()
        # max_sum = result['unmatched'].max()
        # result['norm_sum'] = (result['unmatched'] - min_sum) / (max_sum - min_sum)

        result["proportion"] = result["unmatched"] / result["sum"]
        # result["weighted_prop"] = result["proportion"] * result["norm_sum"]
        result.sort_values("proportion", ascending=False, inplace=True)
        # result["weighted_prop"] = result["weighted_prop"].round(2)
        result["proportion"] = result["proportion"].round(2)

        result.to_csv("authors/authors-prefixes-proportion.csv")

        ## API

        with open("authors/author-prefixes-fixed.txt", "r") as r:
            fixed_counts = [(i.split(": ")) for i in r.read().splitlines()]

        fixed = pd.DataFrame.from_records(fixed_counts, columns=["doi", "fixed"])
        fixed["fixed"] = fixed["fixed"].str.replace(",", "").astype(int)

        with open("authors/author-prefixes-not_fixed.txt", "r") as r:
            not_fixed_counts = [
                (i.split(". ", 1)[0].split(": ")) for i in r.read().splitlines()
            ]

        notfixed = pd.DataFrame.from_records(
            not_fixed_counts, columns=["doi", "notfixed"]
        )
        notfixed["notfixed"] = (
            notfixed["notfixed"].str.replace(",", "").astype(int)
        )

        result = pd.merge(fixed, notfixed, on="doi", how="outer")

        result.set_index('doi', inplace=True)
        result = result.fillna(0).astype(int)
        result["sum"] = result["notfixed"] + result["fixed"]
        
        result = result[result["sum"] > 50]

        # min_sum = result['unmatched'].min()
        # max_sum = result['unmatched'].max()
        # result['norm_sum'] = (result['unmatched'] - min_sum) / (max_sum - min_sum)

        result["proportion"] = result["notfixed"] / result["sum"]
        # result["weighted_prop"] = result["proportion"] * result["norm_sum"]
        result.sort_values("proportion", ascending=False, inplace=True)
        # result["weighted_prop"] = result["weighted_prop"].round(2)
        result["proportion"] = result["proportion"].round(3)

        result.to_csv("authors/authors-prefixes-proportion-api.csv")

    if False:  ## use similarity metric
        output = {
            k: v for k, v in filtered.items() if min_similarity(list(v.keys())) < 0.2
        }

        with open("authors/authors-duplicates.json", "w", encoding="utf-8") as w:
            json.dump(output, w, ensure_ascii=False)

    if False:  ## see if they're fixed
        with open("authors/crossref-1-0.json", "r") as r:
            data = json.load(r)

        if False:  ## annotate
            with open("authors/orcid-api-all.pickle", "rb") as r:
                api = pickle.load(r)

            annotated = {}
            count = 0
            for k in data.keys():
                if k in api:
                    if (
                        len(api[k]["name"]) > 0
                        or len(api[k]["other_names"]) > 0
                        or len(api[k]["credit_name"]) > 0
                    ):
                        annotated[k] = {"data": data[k], "orcid": api[k]}
                    else:
                        count += 1

            print(f"ORCIDs without any name: {count}\n")

            with open(
                "authors/authors-annotated-api-all.json", "w", encoding="utf-8"
            ) as w:
                json.dump(annotated, w, ensure_ascii=False)

            # with open("authors/authors-annotated-api-all.pickle", "wb") as w:
            #     pickle.dump(annotated, w)
        else:
            with open("authors/authors-annotated-api-all.pickle", "rb") as r:
                annotated = pickle.load(r)

                if True:  ## fix credit name 🤷
                    for v in annotated.values():
                        if "credit_name" not in v["orcid"]:
                            v["orcid"]["credit_name"] = ""

                    with open("authors/authors-annotated-api-all.pickle", "wb") as w:
                        pickle.dump(annotated, w)

        output = {
            k: v
            for k, v in annotated.items()
            if min_similarity(
                list(v["data"].keys()),
                [
                    v["orcid"]["name"],
                    *v["orcid"]["other_names"],
                    v["orcid"]["credit_name"],
                ],
            )
            < 0.2
        }

        if False:  ## one line
            for o, d in output.items():
                for k, v in d["data"].items():
                    output[o]["data"][k] = v[0]

        with open(
            "authors/authors-not-fixed-api.json",
            "w",
            encoding="utf-8",
        ) as w:
            json.dump(output, w, ensure_ascii=False)


def visualise_simlarity():
    if False:
        similarity_counts = Counter()

        for _, v in filtered.items():
            similarity_counts[round(min_similarity(list(v.keys())), 2)] += 1

        with open("authors/similarities.csv", "w", newline="") as w:
            writer = csv.writer(w)
            writer.writerows(sorted(similarity_counts.items()))

    df = pd.read_csv("authors/similarities.csv", names=["v", "c"])
    fig, ax = plt.subplots()

    ax.bar(df.v, df.c, width=0.005)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylabel("Number of occurences")
    ax.set_xlabel(
        "Similarity score",
    )

    ax.yaxis.set_major_formatter(tkr.FuncFormatter(lambda x, _: format(int(x), ",")))

    plt.gcf().set_size_inches(10, 6.6)

    plt.savefig(
        "similarities.png",
        bbox_inches="tight",
        pad_inches=0.25,
        dpi=200,
        format="png",
    )
    plt.close(fig)


def definite_fails():
    if False:  ## author names with ", " in (list of names)
        with open("authors/authors.pickle", "rb") as r:
            data = pickle.load(r)

        commas = {}
        for k, v in data.items():
            for name in v.keys():
                if ", " in name:
                    commas[k] = {"data": v}
                    break

        with open("authors/authors-with-commas.json", "w", encoding="utf-8") as w:
            json.dump(commas, w, ensure_ascii=False)

    if False:  ## ORCID urls which fail
        with open("authors/authors.pickle", "rb") as r:
            data = pickle.load(r)

        with open("authors/api/failed.txt", "r") as file:
            orcid_urls = [line.strip() for line in file]

        failed = {}

        for orcid in tqdm(orcid_urls, miniters=1):
            # print(orcid)
            orcid_id = orcid.split("/")[-1]

            # Construct the ORCID API endpoint URL
            api_url = f"https://pub.orcid.org/v3.0/{orcid_id}/person"

            # Set the headers for the API request
            headers = {
                "Accept": "application/json",
                "Authorization": "Bearer b3deddaa-b357-4cb3-93dd-66290bfdd981",
            }

            # Send the API request and retrieve the response
            try:
                response = requests.get(api_url, headers=headers)

                if response.status_code != 200:
                    failed[orcid] = {"data": data[orcid]}
            except Exception as e:
                failed[orcid] = {"data": data[orcid]}

        with open("authors/orcids-failed.json", "w", encoding="utf-8") as w:
            json.dump(failed, w, ensure_ascii=False)

    if False:  ## without name and deactivated
        with open("authors/authors.pickle", "rb") as r:
            data = pickle.load(r)

        with open("authors/orcid-api-all.pickle", "rb") as r:
            api = pickle.load(r)

        if False:  ## orcids without names
            without_name = {}
            for k in api.keys():
                if (
                    len(api[k]["name"]) == 0
                    and len(api[k]["other_names"]) == 0
                    and len(api[k]["credit_name"]) == 0
                ):
                    without_name[k] = {"data": data[k], "orcid": api[k]}

            with open("authors/orcids-without-name.json", "w", encoding="utf-8") as w:
                json.dump(without_name, w, ensure_ascii=False)

        if True:  ## deactivated
            deactivated = {}
            for k in api.keys():
                if api[k]["name"] == "Given Names Deactivated Family Name Deactivated":
                    deactivated[k] = {"data": data[k], "orcid": api[k]}

            with open("authors/orcids-deactivated.json", "w", encoding="utf-8") as w:
                json.dump(deactivated, w, ensure_ascii=False)


def test_set() -> None:
    with open("authors/authors.pickle", "rb") as r:
        data = pickle.load(r)
    with open("authors/authors-test.txt", "r") as r:
        tests = [i.strip().split(",") for i in r.readlines()]

    test_output = {}
    for t, expected in tests:
        test_output[t] = {"expected": expected}
        if t in data:
            test_output[t]["canon_similarity"] = round(
                min_similarity(list(data[t].keys()), [expected]), 2
            )
            if len(data[t]) > 1:
                test_output[t]["similarity"] = round(
                    min_similarity(list(data[t].keys())), 2
                )
            test_output[t] = test_output[t] | data[t]
        else:
            test_output[t] = "Not found"

    with open("authors/authors-report-test-new.json", "w", encoding="utf-8") as w:
        json.dump(test_output, w, ensure_ascii=False)


def get_orcid_report() -> None:
    data_dir = "crossref/data/"
    directory = data_dir + "April 2023 Public Data File from Crossref/"

    orcids = {
        "none": 0,
        "any": 0,
        "all": 0,
        "orcid-count": 0,
        "authenticated": 0,
        "authors": 0,
        "item-count": 0,
    }

    # it = 0

    for filename in tqdm(os.listdir(directory)):
        f = os.path.join(directory, filename)

        with gzip.open(f, "r") as r:
            data = json.loads(r.read().decode("utf-8"))

        for o in data:
            for i in data[o]:
                if "author" in i and len(i["author"]) > 0:
                    orcids["item-count"] += 1
                    any = 0
                    all = 1

                    for k in i["author"]:
                        orcids["authors"] += 1
                        if "ORCID" in k:
                            any = 1
                            orcids["orcid-count"] += 1
                            if k["authenticated-orcid"] == True:
                                orcids["authenticated"] += 1
                        else:
                            all = 0

                    if all == 1:
                        orcids["all"] += 1
                    if any == 1:
                        orcids["any"] += 1
                    else:
                        orcids["none"] += 1

        # if it > 5:
        #     break
        # it += 1

    with open("authors/orcid-report.txt", "w") as w:
        w.write(f"Items: {orcids['item-count']:,}\n")
        w.write(f"Authors (not unique): {orcids['authors']:,}\n")
        w.write(f"ORCIDs (not unique): {orcids['orcid-count']:,}\n")
        w.write(f"Authenticated ORCIDs (not unique): {orcids['authenticated']:,}\n\n")
        w.write(f"Papers where:\n")
        w.write(
            f"\tNo author has an ORCID: {orcids['none']:,} ({orcids['none']/ orcids['item-count']*100:.1f}%)\n"
        )
        w.write(
            f"\tAny author has an ORCID: {orcids['any']:,} ({orcids['any']/ orcids['item-count']*100:.1f}%)\n"
        )
        w.write(
            f"\t\tAll authors have ORCIDs: {orcids['all']:,} ({orcids['all']/ orcids['item-count']*100:.1f}%)\n"
        )


def orcid_report() -> None:
    with open("authors/authors.pickle", "rb") as r:
        data = pickle.load(r)

    orcids = len(data.keys())

    author_strings = 0
    authors_with_commas = 0
    orcid_counts = Counter()
    for k, v in data.items():
        author_strings += len(v)
        orcid_counts[k] = len(v)
        for name in v.keys():
            if ", " in name:
                authors_with_commas += 1

    one = len([i for i in orcid_counts if orcid_counts[i] == 1])
    two = len([i for i in orcid_counts if orcid_counts[i] == 2])
    greater_than = len([i for i in orcid_counts if orcid_counts[i] > 2])

    with open("authors/authors-report-test.txt", "w") as w:
        w.write("Items: 115,845,460\n\n")

        w.write("Author name strings (not unique): 390,050,406\n")
        w.write(f"Author name strings (unique): {author_strings:,}\n")
        w.write(f"Number of items per author: {390050406 /author_strings:.1f}\n")
        w.write(f"Number of authors per item: {author_strings/ 115845460:.1f}\n")

        w.write("ORCIDs (not unique): 22,060,154\n")
        w.write(f"Number of ORCIDs (unique): {orcids:,}\n")
        w.write(f"Number of items per ORCID: {22060154 /orcids:.1f}\n")
        w.write("Authenticated ORCIDs (not unique): 3,076,815 (14%)\n\n")

        w.write("Papers where:\n")
        w.write("    No author has an ORCID: 105,449,629 (91%)\n")
        w.write("    Any author has an ORCID: 10,395,831 (9%)\n")
        w.write("        All authors have ORCIDs: 2,620,695 (2.3%)\n\n")

        w.write(f"Average number of names per ORCID: {author_strings / orcids:.02}\n")
        w.write(f"ORCIDs with one author: {one:,} ({one/ orcids*100:.0f}%)\n")
        w.write(
            f"ORCIDs with two author name strings: {two:,} ({two/ orcids*100:.0f}%)\n"
        )
        w.write(
            f"ORCIDs with more than two author name strings: {greater_than:,} ({greater_than/ orcids*100:.0f}%)\n"
        )
        w.write(f"\nTop ten ORCIDs with the most author name strings:\n")
        for s, c in orcid_counts.most_common(10):
            w.write(f"\t{s}: {c:,}\n")
        w.write(
            f"\nAuthors name strings with ', ' (i.e., a list of names): {authors_with_commas:,}\n"
        )

        w.write(f"Number of ORCIDs which have duplicate DOIs: {1:,}\n")
        w.write(f"\tNumber of DOIs for which any ORCID occurs more than once: {1:,}\n")
        w.write(f"Number of ORCIDs with string similarity less than 0.2: {1:,}\n")

    if False:  # Prefix report
        # Number of papers
        with open("authors/authors-duplicates.json", "r") as r:
            duplicates = json.load(r)

        output = set()
        prefix_counts = Counter()

        for o in duplicates.keys():

            for n, d in duplicates[o].items():
                for s in d:
                    output.add(s)
                    prefix_counts[s.split("/")[0]] += 1

        print(len(output))
        with open(f"authors/author-prefixes.txt", "w") as w:
            for prefix, count in sorted(
                prefix_counts.items(), key=lambda x: x[1], reverse=True
            ):
                w.write(f"{prefix}: {count:,}\n")


def deduplicate():
    # orcid_file = "authors-annotated-duplicates-all"
    orcid_file = "authors-not-fixed-api"

    multiple_author_files = [
        "authors-duplicates",
        "authors-duplicates-confirmed",
    ]

    definite_files = [
        "orcids-deactivated",
        "orcids-without-name",
        "authors-with-commas",
        "orcids-failed",
    ]

    affected_orcids = set()
    affected_dois = set()

    with open(f"authors/{orcid_file}.json", "r") as r:
        data = json.load(r)

    for k, v in data.items():
        name_strings = list(v["data"].keys())
        canonical_strings = [
            v["orcid"]["name"],
            *v["orcid"]["other_names"],
            v["orcid"]["credit_name"],
        ]
        canonical_strings = [c for c in canonical_strings if len(c) > 0]
        if len(canonical_strings) > 0:
            for name in name_strings:
                max_sim = 0
                for canon in canonical_strings:
                    s = similarity(name, canon)

                    if s > max_sim:
                        max_sim = s

                if max_sim < 0.2:
                    affected_orcids.add(k)

                    for doi in v["data"][name]:
                        affected_dois.add(doi)

    # for f in definite_files:
    #     with open(f"authors/{f}.json", "r") as r:
    #         data = json.load(r)

    #     for o, values in data.items():
    #         affected_orcids.add(o)
    #         for name, dois in values["data"].items():
    #             for doi in dois:
    #                 affected_dois.add(doi)

    with open("authors/affected_orcids.txt", "w") as w:
        for d in affected_orcids:
            w.write(d + "\n")

    with open("authors/affected_dois.txt", "w") as w:
        for d in affected_dois:
            w.write(d + "\n")


def similarity(string: str, canonical: str) -> float:
    string = unidecode(string.casefold())
    canonical = unidecode(canonical.casefold())
    matcher = SequenceMatcher(lambda x: x == " ", string, canonical)
    return matcher.ratio()


def min_similarity(strings: list[str], canonical: list[str] = []) -> float:
    if len(strings) < 2 and not canonical:
        return 0

    strings = [unidecode(s.casefold()) for s in strings]

    if canonical:
        min_sim = 1
        canonical = [unidecode(s.casefold()) for s in canonical]

        # max then min
        # find max similarity with canonical for each string
        # then find the minimum of those maxes
        for i in range(len(strings)):
            max_sim = 0
            for j in range(len(canonical)):
                matcher = SequenceMatcher(lambda x: x == " ", strings[i], canonical[j])
                similarity = matcher.ratio()
                # total_similarity += similarity
                # total_pairs += 1
                if similarity > max_sim:
                    max_sim = similarity
            if max_sim < min_sim:
                min_sim = max_sim

    else:
        # total_similarity = 0
        # total_pairs = 0
        min_sim = 1
        for i in range(len(strings)):
            for j in range(i + 1, len(strings)):
                matcher = SequenceMatcher(lambda x: x == " ", strings[i], strings[j])
                similarity = matcher.ratio()
                # total_similarity += similarity
                # total_pairs += 1
                if similarity < min_sim:
                    min_sim = similarity

    # average_similarity = total_similarity / total_pairs
    return min_sim


def fixed_dois():
    if False:
        with open("authors/affected_dois_for_api.txt", "r") as r:
            first = set(r.read().splitlines())
        with open("authors/affected_dois.txt", "r") as r:
            second = set(r.read().splitlines())

        fixed = first - second

        with open("authors/affected_dois_fixed.txt", "w") as w:
            for d in fixed:
                w.write(d + "\n")

    if True:  # Prefix report
        cases = ["fixed", "not_fixed"]
        for case in cases:
            with open(f"authors/affected_dois_{case}.txt", "r") as r:
                data = set(r.read().splitlines())

            prefix_counts = Counter()

            for o in data:
                prefix_counts[o.split("/")[0]] += 1

            with open(f"authors/author-prefixes-{case}.txt", "w") as w:
                for prefix, count in sorted(
                    prefix_counts.items(), key=lambda x: x[1], reverse=True
                ):
                    w.write(f"{prefix}: {count:,}\n")


def transform_authors() -> None:
    with open("authors/authors-duplicates.json", "r") as r:
        data = json.load(r)

    for o, d in data.items():
        for k, v in d.items():
            data[o][k] = v[0]

    with open("authors/authors-duplicates-one-doi.json", "w", encoding="utf-8") as w:
        json.dump(data, w, ensure_ascii=False)


if __name__ == "__main__":
    tick = time.time()
    # consolidate_orcids()
    find_duplicate_orcids()
    # visualise_simlarity()
    # definite_fails()
    # test_set()
    # get_orcid_report()
    # orcid_report()

    # deduplicate()
    # fixed_dois()

    # transform_authors()
    print(f"Elapsed time: {time.time() - tick:.5f} seconds")
