from collections import Counter

import requests

filename = "no-qid-for-license.txt"


def getCounts():

    # Initialize a counter to store the frequencies of the URLs
    url_counts = Counter()

    # Open the file and read the URLs
    with open(filename, "r") as r:
        for line in r:
            # Increment the counter for the URL
            url_counts[line.strip()] += 1

    # Write the results to a file in descending order
    with open("url_counts.txt", "w") as w:
        for url, count in sorted(url_counts.items(), key=lambda x: x[1], reverse=True):
            w.write(f"{url}: {count}\n")


def splitURLs():
    # Set up an empty list to store the deduplicated URLs
    deduplicated_urls = []

    # Set up two empty lists to store the URLs with status 404 and the URLs with other statuses
    not_found_urls = []
    other_urls = []

    # Read the URLs from the file
    with open(filename, "r") as r:
        for line in r:
            url = line.strip()
            if url not in deduplicated_urls:
                # Add the URL to the deduplicated list
                deduplicated_urls.append(url)

                # Check the status of the URL
                try:
                    response = requests.get(url)
                    status = response.status_code
                except:
                    # If there is an error, assume the status is 404
                    status = 404

                # Add the URL to the appropriate list based on its status
                if status == 404:
                    not_found_urls.append(url)
                else:
                    other_urls.append(url)

    # Print the results
    print(f"Total number of URLs: {len(deduplicated_urls)}")
    print(f"Number of URLs with status 404: {len(not_found_urls)}")
    print(f"Number of URLs with other statuses: {len(other_urls)}")

    # Write the URLs with status 404 to a file
    with open("404_urls.txt", "w") as w:
        for url in not_found_urls:
            w.write(f"{url}\n")

    # Write the URLs with other statuses to a file
    with open("non_404_urls.txt", "w") as w:
        for url in other_urls:
            w.write(f"{url}\n")


if __name__ == "__main__":
    splitURLs()
