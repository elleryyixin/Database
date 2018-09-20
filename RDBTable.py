import pymysql.cursors

import sys

'''
cnx = pymysql.connect(host='localhost',
                             user='root',
                             password='Cyx1996!',
                             db='Data',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def DBRunQuery(q):
    cursor=cnx.cursor()
    print ("Query = ", q)
    cursor.execute(q);
    r = cursor.fetchall()
    #print("Query result = ", r)
    return r

result = DBRunQuery("select * from people where nameLast='Williams';")
result2 = DBRunQuery("select * from people where nameLast='Williams'")
print result == result2
print("Result = ")
print(result)

'''


class RDBTable():

    def __init__(self, table_name, table_file, key_columns, connect_info):
        '''
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: File name of CSV file to read/write.
        :param key_columns: List of column names the form the primary key.
        '''
        self.table_name = table_name
        self.table_file = table_file
        self.key_columns = key_columns
        self.len_key_columns = len(key_columns)
        self.connect_info = connect_info
        self.cnx = pymysql.connect(host= self.connect_info['host'], user=self.connect_info['user'],
                                   password=self.connect_info['password'], db=self.connect_info['db'],
                                   charset=self.connect_info['charset'],
                                   cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        '''
        Pretty print the table and state.
        :return: String
        '''
        pass

    def load(self):

        '''
        Load information from CSV file.
        :return: None
        '''

        pass

    def find_by_primary_key(self, s, fields=None):
        '''
        Return a table containing the row matching the primary key and field selector.
        :param s: List of strings of primary key values that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: Table containing the answer.
        '''

        try:
            # Check error
            if len(s) != self.len_key_columns:
                print "Primary key length inconsistent"
                return

                t = {}
                for i, val in enumerate(s):
                    t[self.key_columns[i]] = val

                q = self.find_query_generate(t, fields)
                print q

                cursor = self.cnx.cursor()
                cursor.execute(q)
                r = cursor.fetchall()
                print r
                return r

        except Exception as e:
            print e

    def find_by_template(self, t, fields=None):
        '''
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: Table containing the answer.
        '''
        try:
            q = self.find_query_generate(t, fields)
            print q

            cursor = self.cnx.cursor()
            cursor.execute(q)
            r = cursor.fetchall()
            for item in r:
                print item
            if not r:
                print r
            return r

        except Exception as e:
            print e

    def save(self):
        '''
        Write updated CSV back to the original file location.
        :return: None
        '''
        pass

    def insert(self, r):
        '''
        Insert a new row into the table.
        :param r: New row.
        :return: None. Table state is updated.
        '''

        try:
            q = self.insert_query_generation(r)
            with self.cnx.cursor() as cursor:
                cursor.execute(q, r.values())
            self.cnx.commit()

        except Exception as e:
            print e

    def delete(self, t):
        '''
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        '''
        try:
            q = self.delete_query_generation(t)
            with self.cnx.cursor() as cursor:
                cursor.execute(q)
            self.cnx.commit()

        except Exception as e:
            print e

    def wrong_key_input(self, d):

        for k in d:
            if k not in self.keys:
                print "Invalid key input: " + k
                return True

        return False

    def find_query_generate(self, t, fields):
        s = ""
        for k, v in t.items():
            if s != "":
                s += " AND "
            s += k + "='" + v + "'"

        if s != "":
            s = "WHERE " + s;

        if not fields:
            s = "SELECT * FROM " + self.table_name + ' ' + s

        else:
            r = ", ".join(fields)
            s = "SELECT " + r + " FROM " + self.table_name + ' ' + s

        return s

    def insert_query_generation(self, r):
        c = ""
        after_values = ""
        for k, v in r.items():
            if c != "":
                c += ", "
            if after_values != "":
                after_values += ", "

            after_values += "%s"
            c += k

        c = " (" + c + ") "
        after_values = " (" + after_values + ") "

        q = "INSERT INTO " + self.table_name + c + "VALUES" + after_values
        print q
        return q

    def delete_query_generation(self, r):
        s = ""
        for k, v in r.items():
            if s != "":
                s += " AND "
            s += k + "='" + v + "'"

        if s != "":
            s = "WHERE " + s;

        s = "DELETE" + " FROM " + self.table_name + ' ' + s

        print s
        return s


table = RDBTable('people', 'People.csv', ['PlayerID'], {'host': 'localhost', 'user': 'root', 'password': 'Cyx1996!',

                                                        'db': 'Data', 'charset': 'utf8mb4'})

table.find_by_template({'Weight': '215'}, ['nameFirst', 'nameLast'])

table.insert({'PlayerID': 'dff1', 'nameLast': 'Ferguson', 'nameFirst': 'Donald'})

table.find_by_template({'PlayerID': 'dff1'}, ['nameFirst', 'nameLast'])

table.delete({'PlayerID': 'dff1'})

table.find_by_template({'PlayerID': 'dff1'}, ['nameFirst', 'nameLast'])
