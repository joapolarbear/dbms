import gzip
import sys, os
import heapq
import time

from base import decode_data, encode_str, encode_one_row, encode_rows


def cache_partition(buffer, split_path, head):
    # print(split_path)
    split_fp = gzip.open(split_path, 'wb')
    split_fp.write(encode_one_row(head))
    split_fp.write(encode_str("\n"))
    split_fp.write(encode_rows(buffer))
    split_fp.close()

def split_sorted_file(fp, tconst_idx, split_dir, head, split_file_size=1024):
    buffer = []
    buffer_size = 0
    
    split_id = None
    while True:
        line = fp.readline()
        if len(line) == 0:
            ### end of the file
            break

        data = decode_data(line)
        if split_id is None:
            split_id = data[tconst_idx]
        buffer.append(data)
        buffer_size += 1
        if buffer_size >= split_file_size:
            split_path = os.path.join(split_dir, f"{split_id}.tsv.gz")
            cache_partition(buffer, split_path, head)
            buffer = []
            buffer_size = 0
            split_id = None

    if buffer_size > 0:
        split_path = os.path.join(split_dir, f"{split_id}.tsv.gz")
        cache_partition(buffer, split_path, head)

def wrap_split_file(_file, sorted_dir):
    '''
    _file: str
        _file path
    '''
    if not _file.endswith(".tsv.gz"):
        return

    st = time.time()

    split_dir = os.path.join(sorted_dir, os.path.basename(_file).split(".tsv.gz")[0])
    os.makedirs(split_dir)
    with gzip.open(_file, 'rb') as fp:
        print(f"\nSorting {os.path.basename(_file)}")
        head = decode_data(fp.readline())
        print(head)
        if "title.akas" in _file:
            tconst_idx = head.index("titleId")
        elif "name.basics" in _file:
            tconst_idx = head.index("nconst")
        else:
            tconst_idx = head.index("tconst")
        
        ### Split
        split_file_size = 1024
        split_sorted_file(fp, tconst_idx, split_dir, head, split_file_size=split_file_size)

    print(" - Finish spliting {} in {:.3f} s".format(os.path.basename(_file), time.time() - st))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/sorted/data")
        exit(0)

    if not os.path.isdir(sys.argv[1]):
        files = [sys.argv[1]]
    else:
        root, _, files = list(os.walk(sys.argv[1]))[0]
        files = [os.path.join(root, _file) for _file in files]

    sorted_dir = os.path.join(sys.argv[1], "split")
    os.system(f"rm -rf {sorted_dir}")
    os.makedirs(sorted_dir)

    print(f"Split data under {sys.argv[1]} to {sorted_dir}")

    for _file in files:
        wrap_split_file(_file, sorted_dir)
    