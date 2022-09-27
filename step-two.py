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
    data = set()

    # i = 0
    with gzip.open("disambiguated/comm_disambiguated.tsv.gz", "rt") as gf:
        next(gf)
        for line in gf:
            values = line.split("\t")
            disambig = values[13].strip()
            software = values[9].strip()

            if disambig == "not_disambiguated":
                # data.add(software)
                pass
            else:
                data.add(disambig)
            # i += 1
            # if i > 10:
            #     break

    # print(data)
    writeSet("software", data)


def software():
    df = pd.read_csv(
        "linked/metadata.tsv.gz",
        sep="\\t",
        engine="python",
        compression="gzip",
        on_bad_lines="warn",
    )

    # print(df.head())

    # PyPI Index P5568
    pypi = df[df["platform"] == "Pypi"][["mapped_to", "package_url"]]
    pypi.set_index("mapped_to", inplace=True)
    pypi.rename(columns={"package_url": "pypi"}, inplace=True)
    pypi["pypi"] = pypi["pypi"].str.replace(r"h.*\/(.*)", r"\1", regex=True)
    pypi.to_csv("pypi.csv", index=False)

    # Bioconductor Index P10892
    bioconductor = df[df["platform"] == "Bioconductor"][["mapped_to", "package_url"]]
    bioconductor.set_index("mapped_to", inplace=True)
    bioconductor.rename(columns={"package_url": "bioconductor"}, inplace=True)
    bioconductor["bioconductor"] = bioconductor["bioconductor"].str.replace(
        r"h.*\/(.*).html", r"\1", regex=True
    )
    bioconductor.to_csv("bioconductor.csv", index=False)

    # CRAN Index P5565
    cran = df[df["platform"] == "CRAN"][["mapped_to", "package_url"]]
    cran.set_index("mapped_to", inplace=True)
    cran.rename(columns={"package_url": "cran"}, inplace=True)
    cran["cran"] = cran["cran"].str.replace(r"h.*\/(.*)\/index.html", r"\1", regex=True)
    cran.to_csv("cran.csv", index=False)

    partial = pypi.merge(bioconductor, how="outer", left_index=True, right_index=True)
    all_df = partial.merge(cran, how="outer", left_index=True, right_index=True)

    all_df.to_csv("software-ids.csv")


def writeSet(label, data):
    with open(label + "-disambiguated.txt", "w") as w:
        for value in data:
            w.write(value + "\n")


if __name__ == "__main__":
    tick = time.time()
    # main()
    software()
    print("Time: " + str(time.time() - tick))
