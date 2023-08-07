import json
import multiprocessing
import time

import pickle
from typing import Tuple
import requests
from requests.exceptions import SSLError
from tqdm import tqdm
import glob


def get_orcid_info() -> None:
    # Read ORCID URLs from a text file
    with open("authors/api/failed.txt", "r") as file:
        orcid_urls = [line.strip() for line in file]

    # orcid_urls.insert(0, "http://orcid.org/0000-0002-7655-1541")
    orcid_urls = list(reversed(orcid_urls))

    # Partition the URLs into five sections
    num_sections = 6
    section_size = len(orcid_urls) // num_sections
    partitions = [
        orcid_urls[i : i + section_size]
        for i in range(0, len(orcid_urls), section_size)
    ]

    # Create a multiprocessing pool with 5 processes
    pool = multiprocessing.Pool(processes=num_sections)

    # Process ORCID URLs using multiprocessing
    results = []
    skip = 0
    for i, partition in enumerate(partitions):
        result = pool.apply_async(process_orcid_urls, (i + 1, partition[skip:]))
        results.append(result)

    # Wait for all processes to complete
    for result in results:
        result.get()

    # Close the pool
    pool.close()
    pool.join()


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
        if len(v["name"]) > 0 or len(v["other_names"]) > 0 or len(v["credit_name"])> 0
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


def call_API(orcid_url: str) -> Tuple[str, str, list[str]]:
    # Extract ORCID identifier from the URL
    orcid_id = orcid_url.split("/")[-1]

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
    except SSLError:
        with open("authors/api/failed-reason.txt", "a") as a:
            a.write(f"SSLError\n")
        with open("authors/api/failed.txt", "a") as a:
            a.write(f"{orcid_url}\n")
        time.sleep(30)
        return "", "", []
    except Exception as e:
        with open("authors/api/failed-reason.txt", "a") as a:
            a.write(f"{e}\n")
        with open("authors/api/failed.txt", "a") as a:
            a.write(f"{orcid_url}\n")
        time.sleep(30)
        return "", "", []

    # Check if the request was successful
    if response.status_code == 200:
        # print(response.headers)
        # Parse the JSON response
        data = json.loads(response.text)
        name = ""
        credit_name = ""

        # Extract the name
        if data["name"] != None:
            if data["name"]["given-names"] != None:
                name += data["name"]["given-names"]["value"]
                if data["name"]["family-name"] != None:
                    name += " " + data["name"]["family-name"]["value"]
            else:
                if data["name"]["family-name"] != None:
                    name += data["name"]["family-name"]["value"]

            if "credit-name" in data["name"] and data["name"]["credit-name"] != None:
                credit_name = data["name"]["credit-name"]["value"]

        # Extract the other names
        other_names = [name["content"] for name in data["other-names"]["other-name"]]

        return name, credit_name, other_names
    else:
        with open("authors/api/failed.txt", "a") as a:
            a.write(f"{orcid_url}\n")
        return "", "", []


def process_orcid_urls(worker_num: int, orcid_urls: list[str]) -> None:
    orcid_data = {}
    it = 0
    t = tqdm(total=len(orcid_urls), miniters=1, desc=f"Worker {worker_num}", position=worker_num-1)
    time.sleep(worker_num)

    # Iterate over the ORCID URLs and retrieve the information
    for url in orcid_urls:
        name, credit_name, other_names = call_API(url)
        t.update(1)
        orcid_data[url] = {
            "name": name,
            "credit_name": credit_name,
            "other_names": other_names,
        }

        # Output the data as JSON every 10k ORCIDs
        if len(orcid_data) % 10000 == 0:
            output_json(worker_num, it, orcid_data)
            it += 1
            orcid_data = {}

    # Output any remaining data
    if orcid_data:
        output_json(worker_num, it, orcid_data)


def output_json(worker_num: int, iterator: int, data: dict) -> None:
    with open(
        f"authors/api/orcid-{worker_num}-{iterator}-failed.json", "w", encoding="utf-8"
    ) as w:
        json.dump(data, w, ensure_ascii=False)


if __name__ == "__main__":
    tick = time.time()

    # get_orcid_info()
    # fix_failed()
    collate()
    print(f"Elapsed time: {time.time() - tick:.5f} seconds")
