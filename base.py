
import gzip
import os

def decode_data(line):
    return line.decode("UTF-8").strip("\n").split("\t")

def encode_str(_str):
    return _str.encode("UTF-8")

def encode_one_row(row):
    return "\t".join(row).encode("UTF-8")

def encode_rows(rows):
    return "\n".join(["\t".join(row) for row in rows]).encode("UTF-8")

class DBTableRow:
    def __init__(self, row, table):
        self.row = row
        self.table = table
        assert len(row) == len(table.head), (row, table.head, table.fp.name)
    
    def __getitem__(self, attr):
        if attr not in self.table.head:
            return None
        return self.row[self.table.head.index(attr)]

class DBTableSplit:
    def __init__(self, table_path):
        self.fp = gzip.open(table_path, "rb")
        self.head = decode_data(self.fp.readline())

    def reset(self):
        self.fp.seek(0)
        self.fp.readline()

    def next_row(self):
        row = self.fp.readline()
        if len(row) == 0:
            return None
        return DBTableRow(decode_data(row), self)
    
    def find_row(self, attr, value):
        self.reset()
        while True:
            row = self.next_row()
            if row is None:
                ### The last row, still can not find
                return None
            
            if row[attr] == value:
                return row
    
    def close(self):
        self.fp.close()
    
    def __del__(self):
        self.close()

class DBTable:
    def __init__(self, table_path):
        self.table_path = table_path
        _, _, files = list(os.walk(self.table_path))[0]
        self.table_split_keys = sorted([file.split(".tsv.gz")[0] for file in files])
        self.total_split_num = len(self.table_split_keys)

        ### Used to traverse the entire Table through all partitioned small tables
        self.table_split_ptr = [0, DBTableSplit(self.ret_table_split_path(0))]

    def ret_table_split_path(self, split_id):
        return os.path.join(self.table_path,
                            f"{self.table_split_keys[split_id]}.tsv.gz")

    def _find_row(self, split_id, attr, value):
        table_split = DBTableSplit(self.ret_table_split_path(split_id))
        row = table_split.find_row(attr, value)
        table_split.close()
        return row

    def find_row_by_sort(self, attr, value):
        ''' For any v in split V_t, s.t.
                V_{t} <= v < V_{t+1}
        '''
        left, right = 0, self.total_split_num
        while right > (left+1):
            mid = (left + right) // 2
            if value < self.table_split_keys[mid]:
                right = mid
            elif value > self.table_split_keys[mid]:
                if mid == self.total_split_num - 1:
                    ### mid point to the last split
                    return self._find_row(mid, attr, value)
                else:
                    ### mid < self.self.total_split_num - 1
                    if value < self.table_split_keys[mid+1]:
                        ### V_{mid} <= value < V_{mid+1}
                        return self._find_row(mid, attr, value)
                    else:
                        left = mid + 1
            else:
                ### Value == self.table_split_keys[mid]
                return self._find_row(mid, attr, value)
        
        return self._find_row(left, attr, value)

    def reset(self):
        self.table_split_ptr = [0, DBTableSplit(self.ret_table_split_path(0))]

    def next_row(self):
        row = self.table_split_ptr[1].next_row()
        while row is None:
            if self.table_split_ptr[0] < (self.total_split_num - 1):
                self.table_split_ptr[0] += 1
                self.table_split_ptr[1] = DBTableSplit(self.ret_table_split_path(self.table_split_ptr[0]))
                row = self.table_split_ptr[1].next_row()
            else:
                break
        return row


class OutputTable:
    def __init__(self, path, pretty, head):
        self.path = path
        self.pretty = pretty
        self.output_fp = open(self.path, 'w')
        if self.pretty:
            from prettytable import PrettyTable
            self.output_table = PrettyTable()
            self.output_table.field_names = head
        else:
            self.output_fp.write("\t".join(head))
        
    def add_row(self, row):
        if self.pretty:
            self.output_table.add_row(row)
        else:
            self.output_fp.write("\n" + "\t".join(row))

    def close(self):
        if self.pretty:
            self.output_fp.write(str(self.output_table))
        self.output_fp.close()
    
    

    


