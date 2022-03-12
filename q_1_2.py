
import sys, os
import gzip
import time

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

pretty_table = False
output_path = "output1.2.txt"
output_head = ["primaryTitle", "parentTconst", "seasonNumber"]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/store/result")
        exit(0)

    output_table = OutputTable(output_path, pretty_table, output_head)
    st = time.time()

    title_basics_table = DBTableSplit(os.path.join(sys.argv[1], "title.basics.tsv.gz"))
    title_episode_table = DBTableSplit(os.path.join(sys.argv[1], "title.episode.tsv.gz"))


    title_episode_row = title_episode_table.next_row()
    title_basics_row = title_basics_table.next_row()
    while title_episode_row is not None:

        episode_number = title_episode_row["episodeNumber"]
        if not episode_number.isdigit() or int(episode_number) != 1:
            title_episode_row = title_episode_table.next_row()
            continue
        
        matched = False
        while title_basics_row is not None:      
            if title_basics_row["tconst"] > title_episode_row["tconst"]:
                ### The following tconst in title.basics would be larger
                break
            if title_basics_row["tconst"] == title_episode_row["tconst"]:
                ### Found
                matched = True
                break
            title_basics_row = title_basics_table.next_row()
        
        if title_basics_row is None:
            ### Fail to find corresponding tconst in title.basics
            # stop since the following tconst in title.episode would be larger
            break
        if matched:
            output_table.add_row([title_basics_row["primaryTitle"], title_episode_row["parentTconst"], title_episode_row["seasonNumber"]])
        title_episode_row = title_episode_table.next_row()

    output_table.close()
    print("Finish Q1.2 in {:.3f} s".format(time.time() - st))