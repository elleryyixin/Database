import csv          # Python package for reading and writing CSV files.
import os

# You can change to wherever you want to place your CSV files.
rel_path = os.path.realpath('./Data')

class CSVTable():

    # Change to wherever you want to save the CSV files.
    data_dir = rel_path + "/"

    def __init__(self, table_name, table_file, key_columns):
        '''
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: File name of CSV file to read/write.
        :param key_columns: List of column names the form the primary key.
        '''

        # Check error

        if len(key_columns) != len(set(key_columns)):
            print "Duplicate primary key"
            return

        self.table_name = table_name
        self.table_file = self.data_dir + table_file
        self.key_columns = key_columns
        self.len_key_columns = len(key_columns)
        self.keys = []
        self.table = []
        self.updated = False

    def __str__(self):
        '''
        Pretty print the table and state.
        :return: String
        '''
        s = ""
        for row in self.table:
            s += str(row) + '\n'
        return s

    def load(self):
        '''
        Load information from CSV file.
        :return: None
        '''
        with open(self.table_file) as csvfile:
            readf = csv.DictReader(csvfile)

            for row in readf:
                self.table.append(row)

        print self.table[0]
        self.keys = self.table[0].keys()

        # Check error for primary keys
        for k in self.key_columns:
            if k not in self.keys:
                print "Key column inserted not found in file columns: " + k
                return

    def find_by_primary_key(self, s, fields=None):
        '''
        Return a table containing the row matching the primary key and field selector.
        :param s: List of strings of primary key values that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        '''

        # Check error
        if len(s) != self.len_key_columns:
            print "Primary key length inconsistent"
            return

        if self.wrong_key_input(fields):
            return

        ret = {}
        for row in self.table:
            found = True
            for i, v in enumerate(s):
                if row[self.key_columns[i]] != v:
                    found = False
                    break

            if found:
                for f in fields:
                    ret[f] = row[f]
                return ret

    def find_by_template(self, t, fields=None):
        '''
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        '''

        # Check error
        if self.wrong_key_input(t.keys()):
            return

        if self.wrong_key_input(fields):
            return

        ret = []
        for row in self.table:
            found = True
            for k, v in t.items():
                if row[k] != v:
                    found = False
                    break
            if found:
                toappend = {}
                for f in fields:
                    toappend[f] = row[f]
                ret.append(toappend)

        return ret

    def save(self):
        '''
        Write updated CSV back to the original file location.
        :return: None
        '''

        if not self.updated:
            print "No save needed"
            return

        with open(self.table_file, 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=self.keys)
            writer.writeheader()
            for row in self.table:
                writer.writerow(row)

        self.updated = False

    def insert(self, r):
        '''
        Insert a new row into the table.
        :param r: New row.
        :return: None. Table state is updated.
        '''

        # Check error
        if self.wrong_key_input(r.keys()):
            return

        for k in self.key_columns:
            val = r.get(k, None)
            if not val:
                print "Primary key value not inserted: " + k
                return

        for row in self.table:
            for key in self.key_columns:
                if row[key] == r[key]:
                    print "Duplicate primary key values: " + key
                    return

        toappend = {}
        for key in self.keys:
            if key in r.keys():
                toappend[key] = r[key]
            else:
                toappend[key] = None

        self.table.append(toappend)
        self.updated = True

    def delete(self, t):
        '''
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        '''

        if self.wrong_key_input(t.keys()):
            return

        for idx, row in enumerate(self.table):
            found = True
            for k,v in t.values():
                if row[k] != v:
                    found = False
                    break
            if found:
                del self.table[idx]

        self.updated = True

    def wrong_key_input(self, d):

        for k in d:
            if k not in self.keys:
                print "Invalid key input: " + k
                return True

        return False


