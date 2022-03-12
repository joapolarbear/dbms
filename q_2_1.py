
import sys, os
import gzip
import time
import math

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

output_path = "output2.1.txt"
output_fp = open(output_path, 'w')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/store/result")
        exit(0)
    
    st = time.time()
    rst = {}
    title_basics_table = DBTableSplit(os.path.join(sys.argv[1], "title.basics.tsv.gz"))
    title_ratings_table = DBTableSplit(os.path.join(sys.argv[1], "title.ratings.tsv.gz"))

    title_ratings_row = title_ratings_table.next_row()
    title_basics_row = title_basics_table.next_row()
    while title_ratings_row is not None:
        matched = False
        while title_basics_row is not None:
            if title_basics_row["tconst"] > title_ratings_row["tconst"]:
                ### The following tconst in title.basics would be larger
                break
            if title_basics_row["tconst"] == title_ratings_row["tconst"]:
                ### Found
                matched = True
                break
            title_basics_row = title_basics_table.next_row()
        
        if title_basics_row is None:
            ### Fail to find corresponding tconst in title.basics
            # stop since the following tconst in title.crew would be larger
            break
        
        if matched:
            rating = float(title_ratings_row["averageRating"])
            rating = round(rating * 10) / 10.
            assert rating > 0
            rating = 1 if rating == 0 else math.ceil(rating)
            if rating not in rst:
                rst[rating] = 0
            rst[rating] += 1

        title_ratings_row = title_ratings_table.next_row()
    
    for rating in sorted(rst.keys()):
        logstr = "{:.1f} - {:.1f} : {}".format(rating-0.9, rating, rst[rating])
        print(logstr)
        output_fp.write(logstr+"\n")
    
    output_fp.close()
    print("Finish Q2.1 in {:.3f} s".format(time.time() - st))

