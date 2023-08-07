import glob
import json
import multiprocessing
import pickle
import time
from typing import Tuple
import gzip
import os

import requests
from requests.exceptions import SSLError
from tqdm import tqdm
from hashlib import md5


def get_orcid_info() -> None:
    # Read DOIs from a text file
    with open("authors/affected_dois_for_api.txt", "r") as file:
        dois = [line.strip() for line in file]

    process_dois(1, dois)


def fix_failed() -> None:
    failed = glob.glob("authors/api/orcid-*failed.json")
    output = {}
    for filename in failed:
        with open(filename, "r") as r:
            data = json.load(r)

        output = output | data

    output = {
        k: v
        for k, v in output.items()
        if len(v["name"]) > 0 or len(v["other_names"]) > 0 or len(v["credit_name"]) > 0
    }

    with open("authors/api/orcid-failed.json", "w", encoding="utf-8") as w:
        json.dump(output, w, ensure_ascii=False)


def collate() -> None:
    datafiles = glob.glob("authors/api/orcid-*.json")
    output = {}
    for filename in datafiles:
        with open(filename, "r") as r:
            data = json.load(r)

        output = output | data

    print(f"Number of ORCIDs: {len(output)}\n")
    with open("authors/orcid-api-all.pickle", "wb") as w:
        pickle.dump(output, w)


def local_sample():
    directory = "crossref/data/April 2023 Public Data File from Crossref/"
    output = {}
    ot = -1

    t = tqdm(total=29000, miniters=1)
    for filename in os.listdir(directory):
        t.update(1)
        f = os.path.join(directory, filename)
        ot += 1
        if ot % 10 != 0:
            continue

        try:
            with gzip.open(f, "r") as r:
                data = json.loads(r.read().decode("utf-8"))
        except:
            print("😳")
            print(f)
            continue

        it = 0
        for o in data:
            for i in data[o]:
                if "author" in i:
                    output[i["DOI"]] = md5(bytes(str(i["author"]), "utf-8")).hexdigest()

                    it += 1
                    if it >= 100:
                        break

    with open("crossref/data/sample.json", "w", encoding="utf-8") as w:
        json.dump(output, w, ensure_ascii=False)


def sample_api():
    with open("crossref/data/sample.json", "r") as r:
        data = json.load(r)

    data = [(doi, hash_str) for doi, hash_str in data.items()]
    # data = data[:40]

    # Partition the URLs into five sections
    num_sections = 5
    section_size = len(data) // num_sections
    partitions = [data[i : i + section_size] for i in range(0, len(data), section_size)]

    # Create a multiprocessing pool with 5 processes
    pool = multiprocessing.Pool(processes=num_sections)

    # Process ORCID URLs using multiprocessing
    results = []
    skip = 0
    for i, partition in enumerate(partitions):
        result = pool.apply_async(sample_api_worker, (i + 1, partition[skip:]))
        results.append(result)

    # Wait for all processes to complete
    for result in results:
        result.get()

    # Close the pool
    pool.close()
    pool.join()


def sample_api_worker(worker_num: int, data: list[tuple[str, str]]) -> None:
    results = []
    t = tqdm(
        total=len(data),
        miniters=1,
        desc=f"Worker {worker_num}",
        position=worker_num - 1,
    )
    time.sleep(worker_num)
    for doi, hash_str in data:
        url = f"https://api.crossref.org/works/{doi}"

        try:
            response = requests.get(url)
        except Exception as e:
            with open("authors/api/XR-failed-reason.txt", "a") as a:
                a.write(f"{e}\n")
            with open("authors/api/XR-failed.txt", "a") as a:
                a.write(f"{doi}\n")
            time.sleep(30)
            continue

        if response.status_code == 200:
            obj = response.json()
            obj = obj["message"]

            # if flag == True:
            #     with open("sample-api.json", "w", encoding="utf-8") as w:
            #         json.dump(data["author"], w, ensure_ascii=False)
            #     flag = False
            if "author" in obj:
                api_hash = md5(bytes(str(obj["author"]), "utf-8")).hexdigest()
                # print(hash_str == api_hash)
                results.append(hash_str == api_hash)
            else:
                with open("authors/api/XR-failed-no-author.txt", "a") as a:
                    a.write(f"{doi}\n")

        t.update(1)

    with open(f"crossref/data/result-sample-api-{worker_num}.txt", "w") as w:
        for d in results:
            w.write(str(d) + "\n")


def process_dois(worker_num: int, dois: list[str]) -> None:
    it = 0
    t = tqdm(
        total=len(dois),
        miniters=1,
        desc=f"Worker {worker_num}",
        position=worker_num - 1,
    )
    time.sleep(worker_num)

    # Iterate over the ORCID URLs and retrieve the information
    authors = {}
    for doi in dois:
        url = f"https://api.crossref.org/works/{doi}"

        try:
            response = requests.get(url)
        except Exception as e:
            with open("authors/api/XR-failed-reason.txt", "a") as a:
                a.write(f"{e}\n")
            with open("authors/api/XR-failed.txt", "a") as a:
                a.write(f"{doi}\n")
            time.sleep(30)
            continue

        if response.status_code == 200:
            data = response.json()
            data = data["message"]
            if "author" in data:
                for k in data["author"]:
                    if "ORCID" in k:
                        given = k.get("given", "")
                        family = k.get("family", "")
                        name = given + " " + family
                        orcid = k["ORCID"]
                        doi = data["DOI"]

                        if orcid in authors:
                            if name in authors[orcid]:
                                authors[orcid][name].append(doi)
                            else:
                                authors[orcid][name] = [doi]
                        else:
                            authors[orcid] = {name: [doi]}
            else:
                with open("authors/api/XR-failed.txt", "a") as a:
                    a.write(f"{doi}\n")

        t.update(1)
        # it += 1
        # if it > 10:
        #     break

        # Output the data as JSON every 10k ORCIDs
        # if len(authors) % 10000 == 0:
        #     output_json(worker_num, it, authors)
        #     it += 1
        #     authors = {}

    # Output any remaining data
    if authors:
        output_json(worker_num, it, authors)


def output_json(worker_num: int, iterator: int, data: dict) -> None:
    with open(
        f"authors/api/crossref-{worker_num}-{iterator}.json", "w", encoding="utf-8"
    ) as w:
        json.dump(data, w, ensure_ascii=False)


if __name__ == "__main__":
    tick = time.time()

    # get_orcid_info()
    # fix_failed()
    # collate()

    # local_sample()
    sample_api()

    print(f"Elapsed time: {time.time() - tick:.5f} seconds")
