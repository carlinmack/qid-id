import pandas as pd
import time
import gzip


# disambiguated_df = pd.read_csv(
#     "disambiguated/comm_disambiguated.tsv.gz",
#     sep="\\t",
#     engine="python",
#     compression="gzip",
# )


def main():
    data = {"pmc": set(), "pm": set(), "doi": set()}
    # i = 0

    with gzip.open("disambiguated/comm_disambiguated.tsv.gz", "rt") as gf:
        next(gf)
        for line in gf:
            values = line.split("\t")

            data["pmc"].add(values[2])
            data["pm"].add(values[3])
            data["doi"].add(values[4])
            # i += 1
            # if i > 10:
            #     break

    for key in data:
        writeSet(key, data[key])


def collate():
    # doi_df = pd.read_csv("qid-doi.csv", engine="python", index_col=0, dtype=str)
    # pm_df = pd.read_csv("qid-pm.csv", engine="python", index_col=0, dtype=str)
    # pmc_df = pd.read_csv("qid-pmc.csv", engine="python", index_col=0, dtype=str)

    # partial = doi_df.merge(pm_df, how="outer", left_index=True, right_index=True)
    # df = partial.merge(pmc_df, how="outer", left_index=True, right_index=True)

    # df.to_csv("qid-doi-pmc-pm.csv")

    pypi_df = pd.read_csv("qid-pypi.csv", engine="python", index_col=0, dtype=str)
    bioc_df = pd.read_csv("qid-bioconductor.csv", engine="python", index_col=0, dtype=str)
    cran_df = pd.read_csv("qid-cran.csv", engine="python", index_col=0, dtype=str)

    partial = pypi_df.merge(bioc_df, how="outer", left_index=True, right_index=True)
    df = partial.merge(cran_df, how="outer", left_index=True, right_index=True)

    df.to_csv("qid-pypi-bioconductor-cran.csv")


def writeSet(label, data):
    with open(label + ".txt", "w") as w:
        for value in data:
            w.write(str(value) + "\n")


if __name__ == "__main__":
    tick = time.time()
    # main()
    collate()
    print("Time: " + str(time.time() - tick))
