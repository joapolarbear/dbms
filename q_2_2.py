
import sys, os
import gzip
import time

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

output_path = "output2.2.txt"
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
            if title_basics_row["startYear"] not in rst:
                rst[title_basics_row["startYear"]] = {"sum": 0, "cnt": 0}
            rst[title_basics_row["startYear"]]["sum"] += float(title_ratings_row["averageRating"])
            rst[title_basics_row["startYear"]]["cnt"] += 1

        title_ratings_row = title_ratings_table.next_row()

    for year in sorted(rst.keys()):
        logstr = "year: {} average rating: {:.1f}".format(year, rst[year]["sum"]/rst[year]["cnt"])
        print(logstr)
        output_fp.write(logstr+"\n")
    
    output_fp.close()
    print("Finish Q2.2 in {:.3f} s".format(time.time() - st))

