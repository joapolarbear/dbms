
import sys, os
import gzip
import time

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

pretty_table = False
output_path = "output1.1.txt"
output_head = ["primaryTitle", "directors"]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/store/result")
        exit(0)

    output_table = OutputTable(output_path, pretty_table, output_head)
    st = time.time()

    title_basics_table = DBTableSplit(os.path.join(sys.argv[1], "title.basics.tsv.gz"))
    title_crew_table = DBTableSplit(os.path.join(sys.argv[1], "title.crew.tsv.gz"))

    
    ''' Previous method, without using of the sorting property of both tables
    title_basics_table = DBTable(os.path.join(sys.argv[1], "title.basics"))
    title_crew_table = DBTable(os.path.join(sys.argv[1], "title.crew"))
    while True:
        title_crew_row = title_crew_table.next_row()
        if title_crew_row is None:
            break

        directors = title_crew_row["directors"]
        if len(directors.split(",")) <= 1:
            continue

        title_basics_row = title_basics_table.find_row_by_sort("tconst", title_crew_row["tconst"])
        if title_basics_row is not None:
            # print([title_basics_row["primaryTitle"], directors])
            output_table.add_row([title_basics_row["primaryTitle"], directors])
    '''

    title_crew_row = title_crew_table.next_row()
    title_basics_row = title_basics_table.next_row()
    while title_crew_row is not None:

        ### Select operator
        directors = title_crew_row["directors"]
        if len(directors.split(",")) <= 1:
            title_crew_row = title_crew_table.next_row()
            continue
        
        ### Join Operator
        matched = False
        while title_basics_row is not None:
            if title_basics_row["tconst"] > title_crew_row["tconst"]:
                ### The following tconst in title.basics would be larger
                break
            if title_basics_row["tconst"] == title_crew_row["tconst"]:
                ### Found
                matched = True
                break
            title_basics_row = title_basics_table.next_row()
        
        if title_basics_row is None:
            ### Fail to find corresponding tconst in title.basics
            # stop since the following tconst in title.crew would be larger
            break
        if matched:
            output_table.add_row([title_basics_row["primaryTitle"], directors])
        title_crew_row = title_crew_table.next_row()

    output_table.close()
    print("Finish Q1.1 in {:.3f} s".format(time.time() - st))
