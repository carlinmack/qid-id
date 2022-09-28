import gzip
from multiprocessing import Value
import time
from warnings import warn

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

            software = values[9]
            curated = values[12]
            mapped_software = values[13].strip()

            #     software 7094578
            # not software 7675631
            # if curated == "software":
            if mapped_software in software_series.index:
                found = []

                if pmid:
                    pmid = int(pmid[:-2])  # remove .0
                    if pmid in pm_series.index:
                        found = [pm_series[pmid], software_series[mapped_software]]
                elif pmcid in pmc_series.index:
                    found = [pmc_series[pmcid], software_series[mapped_software]]
                elif doiid in doi_series.index:
                    found = [doi_series[doiid], software_series[mapped_software]]

                if found and software:
                    found.append(software)
                if found:
                    data.append(found)

                    # if len(data) > 0:
                    #     print(values)
                    #     print(data)
                    #     exit()
                    # i += 1
                    # if i > 1000:
                    #     break

    writeData(data)


def writeData(data):
    with open("qsv1-all.csv", "w") as w:
        for value in data:
            if len(value) == 2:
                w.write(value[0] + "|P4510|" + value[1] + "|S248|Q114078827\n")
            else:
                w.write(
                    f'{value[0]}|P4510|{value[1]}|P1932|"{value[2]}"|S248|Q114078827\n'
                )


if __name__ == "__main__":
    tick = time.time()
    main()
    print("Time: " + str(time.time() - tick))
