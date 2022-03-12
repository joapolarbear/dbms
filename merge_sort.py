import gzip
import sys, os
import heapq
import time

from base import decode_data, encode_str, encode_one_row, encode_rows

tmp_file_cnt = 0
all_tmp_files = []
tmp_dir = ".tmp"

debug = False

def create_new_tmp_file():
    global tmp_file_cnt, tmp_dir
    tmp_file_path = os.path.join(tmp_dir, f"{tmp_file_cnt}.tsv.gz")
    tmp_fp = gzip.open(tmp_file_path, 'wb')
    tmp_file_cnt += 1
    # print(f"write to {tmp_file_path}")
    return tmp_fp, tmp_file_path

def cache_buffer(buffer, tconst_idx, head):
    buffer = sorted(buffer, key=lambda line: line[tconst_idx])    
    ### store the buffer to a temp file
    tmp_fp, tmp_file_path = create_new_tmp_file()
    tmp_fp.write(encode_one_row(head))
    tmp_fp.write(encode_str("\n"))
    tmp_fp.write(encode_rows(buffer))
    all_tmp_files.append(tmp_file_path)
    tmp_fp.close()

def split_large_file(fp, tconst_idx, head, tmp_file_size=1024):
    buffer = []
    buffer_size = 0

    line_num = 0
    while True:
        line = fp.readline()
        if len(line) == 0:
            ### end of the file
            break

        data = decode_data(line)
        buffer.append(data)
        buffer_size += 1
        if buffer_size >= tmp_file_size:
            cache_buffer(buffer, tconst_idx, head)
            buffer = []
            buffer_size = 0
        
        line_num += 1

        ### for debug
        if debug and line_num > 20:
            break

    if buffer_size > 0:
        cache_buffer(buffer, tconst_idx, head)


class MergeSorter:
    def __init__(self, k, tconst_idx, head):
        self.tconst_idx = tconst_idx
        self.head = head

        self.output_block = []
        self.frontier = []
        self.in_process_tmp_file = None

        self.k = k
        root, _, files = list(os.walk(tmp_dir))[0]
        self.all_tmp_files = [os.path.join(root, _file) for _file in files]
        self.tmp_file_cnt = max([int(_file.split(".tsv.gz")[0]) for _file in files]) + 1
    
    def create_output_file(self):
        assert os.path.exists(tmp_dir)
        tmp_file_path = os.path.join(tmp_dir, f"{self.tmp_file_cnt}.tsv.gz")
        tmp_fp = gzip.open(tmp_file_path, 'wb')
        self.tmp_file_cnt += 1
        # print(f"write to {tmp_file_path}")

        tmp_fp.write(encode_one_row(self.head))
        return tmp_fp, tmp_file_path

    def init_k_way(self):
        ### Init k ways, open k temp files
        self.in_process_tmp_file = [None]*self.k
        for input_block_idx in range(self.k):
            try:
                tmp_file_path = self.all_tmp_files.pop(0)
            except IndexError:
                return
            tmp_file = gzip.open(tmp_file_path, 'rb')
            if tmp_file is None:
                ### There is no more tmp files
                return
            ### The first line is head
            tmp_file.readline()
            self.in_process_tmp_file[input_block_idx] = tmp_file
    
    def add_to_frontier(self, input_block_idx):
        tmp_file = self.in_process_tmp_file[input_block_idx]
        if tmp_file is not None:
            _data = tmp_file.readline()
            if len(_data) == 0:
                ### No more data for this way, set it to None to skip it
                self.in_process_tmp_file[input_block_idx] = None
                tmp_file.close()
                return
            data = decode_data(_data)

            # assert len(data) == len(self.head), data

            # print(data, input_block_idx)
            heapq.heappush(self.frontier, (data[self.tconst_idx], data, input_block_idx))

    def merge_sort(self, target_path, output_block_size=1024):
        ''' k-way merge sorting '''
        while len(self.all_tmp_files) > 1:
            ### Merge k-ways

            ### Prepare the output file to store the results for those k-ways
            output_fp, output_file_path = self.create_output_file()

            # print(self.all_tmp_files)

            self.init_k_way()

            ### Initialize the frontier
            for input_block_idx in range(self.k):
                self.add_to_frontier(input_block_idx)
            
            while True:

                ### compare and get the min one in the frontier of the k ways
                try:
                    _, data, input_block_idx = heapq.heappop(self.frontier)
                except IndexError:
                    ### No more data from those k ways, Finish this k way
                    break
                
                # print(data, input_block_idx)

                ### Write to the output block
                self.output_block.append(data)

                ### Read new data from each way to the frontier
                self.add_to_frontier(input_block_idx)

                ### Check whether the ouput block is full
                if len(self.output_block) > output_block_size:
                    output_fp.write(encode_str("\n"))
                    output_fp.write(encode_rows(self.output_block))            
                    self.output_block = []

            ### end-while # finish current k ways
            if len(self.output_block) > 0:
                output_fp.write(encode_str("\n"))
                output_fp.write(encode_rows(self.output_block))            
                self.output_block = []

            ### Add the output file to end of the total tmp file list
            # In case it needs to be merged with other tmp files again
            self.all_tmp_files.append(output_file_path)
            output_fp.close()

            self.output_block = []
            self.frontier = []
            self.in_process_tmp_file = None

        os.system(f"mv {self.all_tmp_files[0]} {target_path}") 

if __name__ == "__main__":
    if len(sys.argv) <= 2:
        print("Usage: python3 xx.py path/to/raw/data path/to/store/result")
        exit(0)

    print(f"Unzip data under {sys.argv[1]} to {sys.argv[2]}")

    if not os.path.isdir(sys.argv[1]):
        files = [sys.argv[1]]
    else:
        root, _, files = list(os.walk(sys.argv[1]))[0]
        files = [os.path.join(root, _file) for _file in files]

    sorted_dir = sys.argv[2]
    if debug:
        sorted_dir = os.path.abspath(sorted_dir)+"_debug"
    os.system(f"rm -rf {sorted_dir}")
    os.makedirs(sorted_dir)

    for _file in files:
        if not _file.endswith(".tsv.gz"):
            continue

        os.system(f"rm -rf {tmp_dir}")
        os.makedirs(tmp_dir)
        tmp_file_cnt = 0
        all_tmp_files = []

        st = time.time()

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
            tmp_file_size = 10 if debug else 1024 * 200
            k = 10 if debug else 1024
            output_block_size = 1024 if debug else 1024 * 200

            split_large_file(fp, tconst_idx, head, tmp_file_size=tmp_file_size)
            print(" - Finish spliting in {:.3f} s, start to merge sort".format(time.time()-st))
            ### Merge Sort
            merge_sorter = MergeSorter(k, tconst_idx, head)
            merge_sorter.merge_sort(
                os.path.join(sorted_dir, os.path.basename(_file)),
                output_block_size=output_block_size)
        
        print(" - Finish sorting {} in {:.3f} s".format(os.path.basename(_file), time.time() - st))
    
