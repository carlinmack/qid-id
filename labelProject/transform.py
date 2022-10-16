import time
import csv
import pandas as pd
import html


def main():
    df = pd.read_csv("quarry-68091.csv")

    # Perform multiple times as strings can be encoded multiple times
    for _ in range(10):
        df['text'] =  df['text'].apply(lambda x: html.unescape(x))

    df['text'] =  df['text'].apply(lambda x: '"' + x + '"')

    df.to_csv("quickstatements.tsv", sep="\t", index=False, header=False, quoting=csv.QUOTE_NONE)


def timer(tick, msg=""):
    print("--- %s %.3f seconds ---" % (msg, time.time() - tick))
    return time.time()


if __name__ == "__main__":
    tick = time.time()
    main()
    timer(tick)
