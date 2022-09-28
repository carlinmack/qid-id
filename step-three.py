import gzip
from multiprocessing import Value
import time

import pandas as pd

# load data
doi_series = pd.read_csv("qid-doi.csv", index_col=1).squeeze()
pmc_series = pd.read_csv("qid-pmc.csv", index_col=1).squeeze()
pm_series = pd.read_csv("qid-pm.csv", index_col=1).squeeze()

software_series = pd.read_csv("qid-software.csv", index_col=1).squeeze()


def main():
    data = []
    # i = 0

    with gzip.open("disambiguated/comm_disambiguated.tsv.gz", "rt") as gf:
        next(gf)
        for line in gf:
            values = line.split("\t")

            pmcid = int(values[2])
            pmid = values[3]
            doiid = str(values[4])

            curated = values[12]
            software = values[13].strip()

            #     software 7094578
            # not software 7675631
            if curated == "software":
                if software in software_series.index:
                    if pmid:
                        pmid = int(pmid[:-2])  # remove .0
                        if pmid in pm_series.index:
                            data.append([pm_series[pmid], software_series[software]])
                    elif pmcid in pmc_series.index:
                        data.append([pmc_series[pmcid], software_series[software]])
                    elif doiid in doi_series.index:
                        data.append([doi_series[doiid], software_series[software]])

                    # i += 1
                    # if i > 20:
                    #     break

    writeData(data)


def writeData(data):
    with open("triples.csv", "w") as w:
        for value in data:
            try:
                w.write(value[0] + ",P4510," + value[1] + "\n")
            except BaseException as err:
                print(value[0])
                print(value[1])
                raise


if __name__ == "__main__":
    tick = time.time()
    main()
    print("Time: " + str(time.time() - tick))
