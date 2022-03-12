import gzip
import sys, os
import heapq
import time

from base import decode_data, encode_str, encode_one_row, encode_rows


def cache_buffer(buffer, fp):
    # print(split_path)
    fp.write(encode_str("\n"))
    fp.write(encode_rows(buffer))

def hash_sorted_file(fp, tconst_idx, hash_dir, head, patition_num=4):
    buffer_dict = {}
    hash_file_dict = {}
    buffer_size_limit = 1024

    def hash_func(value):
        ''' The input value should be a string, 
            return the sum of ascii values and mod patition_num
        '''
        return sum([ord(v) for v in value]) % patition_num
    
    while True:
        line = fp.readline()
        if len(line) == 0:
            ### end of the file
            break

        data = decode_data(line)
        hash_key = hash_func(data[tconst_idx])
        if hash_key not in buffer_dict:
            buffer_dict[hash_key] = []
            hash_file_dict[hash_key] = gzip.open(os.path.join(hash_dir, f"{hash_key}.tsv.gz"), 'wb')
            hash_file_dict[hash_key].write(encode_one_row(head))
        buffer_dict[hash_key].append(data)
        if len(buffer_dict[hash_key]) >= buffer_size_limit:
            cache_buffer(buffer_dict[hash_key], hash_file_dict[hash_key])
            buffer_dict[hash_key] = []

    for hash_key in buffer_dict.keys():
        if len(buffer_dict[hash_key]) > 0:
            cache_buffer(buffer_dict[hash_key], hash_file_dict[hash_key])
        hash_file_dict[hash_key].close()

def wrap_hash_file(_file, hash_root, patition_num = 4):
    ''' Hash a file and store the results under hash root/basename(_file)
    _file: str
        _file path
    '''
    if not _file.endswith(".tsv.gz"):
        return

    st = time.time()

    hash_dir = os.path.join(hash_root, os.path.basename(_file).split(".tsv.gz")[0])
    os.makedirs(hash_dir)
    with gzip.open(_file, 'rb') as fp:
        print(f"\nHash {os.path.basename(_file)}")
        head = decode_data(fp.readline())
        print(head)
        if "title.akas" in _file:
            tconst_idx = head.index("titleId")
        elif "name.basics" in _file:
            tconst_idx = head.index("nconst")
        else:
            tconst_idx = head.index("tconst")
        
        ### Split
        hash_sorted_file(fp, tconst_idx, hash_dir, head, patition_num=patition_num)

    print(" - Finish hashing {} in {:.3f} s".format(os.path.basename(_file), time.time() - st))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 xx.py path/to/sorted/data")
        exit(0)

    if not os.path.isdir(sys.argv[1]):
        files = [sys.argv[1]]
    else:
        root, _, files = list(os.walk(sys.argv[1]))[0]
        files = [os.path.join(root, _file) for _file in files]

    hash_root = os.path.join(sys.argv[1], "hash")
    os.system(f"rm -rf {hash_root}")
    os.makedirs(hash_root)

    print(f"Hash data under {sys.argv[1]} to {hash_root}")

    for _file in files:
        wrap_hash_file(_file, hash_root)
    