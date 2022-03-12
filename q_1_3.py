
import sys, os
import gzip
import time

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

pretty_table = False
output_path = "output1.3.txt"
output_head = ["primaryTitle"]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/store/result")
        exit(0)

    output_table = OutputTable(output_path, pretty_table, output_head)
    st = time.time()

    title_basics_table = DBTableSplit(os.path.join(sys.argv[1], "title.basics.tsv.gz"))
    title_ratings_table = DBTableSplit(os.path.join(sys.argv[1], "title.ratings.tsv.gz"))

    title_basics_row = title_basics_table.next_row()
    title_ratings_row = title_ratings_table.next_row()
    while title_basics_row is not None:
        matched = False
        while title_ratings_row is not None:
            if title_basics_row["tconst"] < title_ratings_row["tconst"]:
                ### The following tconst in title.basics would be larger
                break
            if title_basics_row["tconst"] == title_ratings_row["tconst"]:
                ### Found
                matched = True
                break
            title_ratings_row = title_ratings_table.next_row()
        if not matched:
            output_table.add_row([title_basics_row["primaryTitle"]])
        title_basics_row = title_basics_table.next_row()
    output_table.close()
    print("Finish Q1.3 in {:.3f} s".format(time.time() - st))