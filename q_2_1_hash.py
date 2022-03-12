
import sys, os
import gzip
import time
import math

from base import decode_data, encode_str, encode_one_row, encode_rows
from base import DBTable, OutputTable, DBTableSplit

from hash_table import wrap_hash_file

output_path = "output2.1_hash.txt"
output_fp = open(output_path, 'w')
in_memory_hash = False
in_memory_patition_num = 128

def in_memory_hash_func(value):
    ''' The input value should be a string, 
        return the sum of ascii values and mod patition_num
    '''
    return sum([ord(v) for v in value]) % in_memory_patition_num

def join_patition_in_memory_table(
        title_basics_dir,
        title_ratings_dir,
        title_basics_file,
        rst):
    ### For each partition of title.basics,
    # load the entire partition to the memory
    # and build an in-memory hash table
    in_memeory_hash_table = {}
    title_basics_table = DBTableSplit(os.path.join(title_basics_dir, title_basics_file))
    title_basics_row = title_basics_table.next_row()
    while title_basics_row is not None:
        # Add to the in-memory hash table
        in_memory_hash_key = in_memory_hash_func(title_basics_row["tconst"])
        if in_memory_hash_key not in in_memeory_hash_table:
            in_memeory_hash_table[in_memory_hash_key] = []
        # Perform projection to reduce memory consumption
        in_memeory_hash_table[in_memory_hash_key].append(title_basics_row["tconst"])
        title_basics_row = title_basics_table.next_row()
    title_basics_table.close()

    ### Load each relation in title.ratings one by one
    # to perform join
    ### the partition file with the name as `title_basics_file` must exist
    title_ratings_path = os.path.join(title_ratings_dir, title_basics_file)
    assert os.path.exists(title_ratings_path), title_ratings_path
    title_ratings_table = DBTableSplit(title_ratings_path)

    title_ratings_row = title_ratings_table.next_row()
    while title_ratings_row is not None:
        # print(title_ratings_row["tconst"])
        in_memory_hash_key = in_memory_hash_func(title_ratings_row["tconst"])
        for tconst in in_memeory_hash_table[in_memory_hash_key]:
            if tconst == title_ratings_row["tconst"]:
                ### Found corresponding tconst in title.basics
                rating = float(title_ratings_row["averageRating"])
                rating = round(rating * 10) / 10.
                assert rating > 0
                rating = 1 if rating == 0 else math.ceil(rating)
                if rating not in rst:
                    rst[rating] = 0
                rst[rating] += 1
        title_ratings_row = title_ratings_table.next_row()
    title_ratings_table.close()

def join_patition_merge_join(
        title_basics_dir,
        title_ratings_dir,
        title_basics_file,
        rst):
    title_basics_table = DBTableSplit(os.path.join(title_basics_dir, title_basics_file))
    ### the partition file with the name as `title_basics_file` must exist
    title_ratings_path = os.path.join(title_ratings_dir, title_basics_file)
    if not os.path.exists(title_ratings_path):
        return
    title_ratings_table = DBTableSplit(title_ratings_path)

    title_basics_row = title_basics_table.next_row()
    title_ratings_row = title_ratings_table.next_row()
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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/store/result")
        exit(0)
    
    hash_root = os.path.join(sys.argv[1], "hash")

    title_basics_dir = os.path.join(hash_root, "title.basics")
    title_ratings_dir = os.path.join(hash_root, "title.ratings")

    ### Partition
    if not os.path.exists(title_basics_dir):
        print("Not partitioned, partition tables based on hasing")
        wrap_hash_file(os.path.join(sys.argv[1], "title.basics.tsv.gz"), hash_root, patition_num = 1024)
        wrap_hash_file(os.path.join(sys.argv[1], "title.ratings.tsv.gz"), hash_root, patition_num = 1024)
    else:
        print(f"Already partitioned under {hash_root}")

    rst = {}
    st = time.time()
    
    _, _, title_basics_files = list(os.walk(title_basics_dir))[0]
    for title_basics_file in title_basics_files:
        # print(title_basics_file)
        if in_memory_hash:
            join_patition_in_memory_table(
                title_basics_dir,
                title_ratings_dir,
                title_basics_file,
                rst
            )
        else:
            join_patition_merge_join(
                title_basics_dir,
                title_ratings_dir,
                title_basics_file,
                rst
            )

    for rating in sorted(rst.keys()):
        logstr = "{:.1f} - {:.1f} : {}".format(rating-0.9, rating, rst[rating])
        print(logstr)
        output_fp.write(logstr+"\n")
    
    output_fp.close()
    print("Finish Q2.1 in {:.3f} s (Excluding the cost of patitioning tables)".format(time.time() - st))

