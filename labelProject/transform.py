import time

import pandas as pd
import html


def main():
    df = pd.read_csv("quarry-68091.csv")

    # To add a label in a specific language to an item, use "Lxx" instead of a property, with "xx" as the language code.
    # Example: Q340122 TAB Lpl TAB "Cyprian Kamil Norwid"
    # Meaning: add Polish label "Cyprian Kamil Norwid" to Cyprian Norwid (Q340122)

    df['id'] = 'Q' + df['id'].astype(str)
    df['language'] = 'L' + df['language'].astype(str)
    df['text'] =  df['text'].apply(lambda x: html.unescape(x))
    # 2.3k of these have been encoded twice so transform it again
    df['text'] =  df['text'].apply(lambda x: html.unescape(x))
    # df['text'] =  df['text'].apply(lambda x: '"' + x + '"')

    df.to_csv("quickstatements.tsv", sep="\t", index=False, header=False)


def timer(tick, msg=""):
    print("--- %s %.3f seconds ---" % (msg, time.time() - tick))
    return time.time()


if __name__ == "__main__":
    tick = time.time()
    main()
    timer(tick)
