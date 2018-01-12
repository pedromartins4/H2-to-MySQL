import pymysql
import jaydebeapi


class H2toMySQL:
    h2_connection = None
    mysql_connection = None
    h2_tables = dict()

    def __init__(self):
        print("Connecting to H2 DB '%s'..." % H2_DB_PATH)
        self.h2_connection = jaydebeapi.connect("org.h2.Driver",  # driver class
                                                "jdbc:h2:" + H2_DB_PATH,
                                                ["sa", ""],  # credentials
                                                "./h2-1.4.196.jar", )  # location of H2 jar

        print("Connecting to MySQL DB '%s'" % MYSQL_DB_NAME)
        self.mysql_connection = pymysql.connect(host=MYSQL_DB_HOST, user=MYSQL_DB_USER, password=MYSQL_DB_PASS,
                                                cursorclass=pymysql.cursors.DictCursor)

    def __del__(self):
        if self.h2_connection is not None:
            self.h2_connection.close()
        if self.mysql_connection is not None:
            self.mysql_connection.close()

    # Resets the MySQL DB. Mostly for testing.
    def reset_mysql(self):
        try:
            curs = self.mysql_connection.cursor()
            curs.execute("DROP DATABASE %s;" % MYSQL_DB_NAME)
        except:
            pass  # Likely DB did not exist
        finally:
            curs.close()

    # Creates new MySQL DB if it does not exist.
    def create_new_db(self):
        query_mysql = "SHOW DATABASES"

        try:
            curs = self.mysql_connection.cursor()

            curs.execute(query_mysql)
            res = curs.fetchall()

            tables = list()
            if res is None:
                raise Exception("Unable to obtain DB names from MySQL.")
            else:
                # Each res = {'Database': 'mysql'}
                for r in res:
                    tables.append(r['Database'])

            if MYSQL_DB_NAME in tables:
                raise Exception("Database '%s' already exists." % MYSQL_DB_NAME)

            query_mysql = "CREATE DATABASE %s;"

            curs.execute(query_mysql % MYSQL_DB_NAME)

            self.mysql_connection.select_db(MYSQL_DB_NAME)

        finally:
            if curs is not None:
                curs.close()

    # Converts types as needed, some types are H2 only (eg: VARCHAR(2147483647), REAL are not supported in MySQL)
    # By default behaves like identity function
    def convert_types(self, type):
        if 'VARCHAR' in type:
            return 'TEXT'

        if 'BOOLEAN' in type:
            return 'Boolean'

        if 'DOUBLE' in type:
            value = type[7:-1]  # type = 'DOUBLE(value)'
            return 'FLOAT(%s,%s)' % (value, value)

        if 'REAL' in type:
            return 'FLOAT(10,10)'

        return type

    # Get tables and respective schema from H2
    def get_h2_tables(self):
        query_h2_tables = "SHOW TABLES FROM PUBLIC;"
        query_h2_table_schema = "SHOW COLUMNS FROM %s;"

        try:
            curs = self.h2_connection.cursor()

            curs.execute(query_h2_tables)
            res = curs.fetchall()

            if res is None:
                raise Exception("Unable to obtain tables from '%s'." % H2_DB_PATH)
            else:
                tables = list()
                # Each res = (TABLE, SCHEMA)
                for (t, _) in res:
                    tables.append(t)

            for table in tables:
                curs.execute(query_h2_table_schema % table)

                res = curs.fetchall()

                table_columns = dict()
                # Each r = (FIELD, TYPE, NULL, KEY, DEFAULT)
                for (field_name, type, is_null, is_key, default_value) in res:
                    table_columns[field_name] = {
                        'type': self.convert_types(type), 'is_null': is_null, 'is_key': is_key,
                        'default_value': default_value}

                    self.h2_tables[table] = table_columns

        finally:
            if curs is not None:
                curs.close()

    # Creates table in MySQL
    def create_mysql_table(self, table):
        query_mysql = "CREATE TABLE %s (%s);"

        try:
            mysql_columns = list()
            for column in self.h2_tables[table]:
                temp = "%s %s" % (column, self.h2_tables[table][column]['type'])

                if self.h2_tables[table][column]['is_null'] == 'YES':
                    temp += ' NULL'
                if self.h2_tables[table][column]['is_null'] == 'NO':
                    temp += ' NOT NULL'

                # Causes errors
                # if self.h2_tables[table][column]['is_key'] == 'PRI':
                #     temp += ' PRIMARY KEY'

                # print(temp)
                mysql_columns.append(temp)

            curs = self.mysql_connection.cursor()
            query = query_mysql % (table, ', '.join(mysql_columns))
            curs.execute(query)

        finally:
            if curs is not None:
                curs.close()

    # Escaping for MySQL as seen here
    # https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
    def escape_strings(self, string):
        escaped = string.translate(str.maketrans({"'": "\\'",
                                                  "\"": "\\\"",
                                                  "\\": "\\\\",
                                                  "%": "\\%",
                                                  "_": "\\_"}))
        return escaped

    # Exports a table to MySQL
    # Reading the writing is done in batches with the same size
    def export_h2_table(self, table):

        query_h2_count = "SELECT COUNT(*) FROM %s;"
        query_h2_select = "SELECT * FROM %s OFFSET %s FETCH NEXT %s ROWS ONLY;"

        table_columns = ', '.join(self.h2_tables[table].keys())
        query_mysql_insert = "INSERT INTO " + table + " (" + table_columns + ") VALUES %s;"

        try:
            curs_h2 = self.h2_connection.cursor()
            curs_mysql = self.mysql_connection.cursor()

            curs_h2.execute(query_h2_count % table)
            table_size, = curs_h2.fetchone()
            table_size = int(str(table_size))

            batch = 0
            while batch < table_size:

                batch_export_data = list()
                curs_h2.execute(query_h2_select % (table, batch, BATCH_SIZE))
                for results in curs_h2.fetchall():
                    results = list(results)
                    res = '(' + ', '.join(map(lambda x: "'" + self.escape_strings(str(x)) + "'", results)) + ')'
                    batch_export_data.append(res)

                if batch + BATCH_SIZE >= table_size:
                    batch = table_size
                else:
                    batch += BATCH_SIZE

                query = query_mysql_insert % ', '.join(batch_export_data)
                curs_mysql.execute(query)
                self.mysql_connection.commit()

                print("  %s out of %s rows inserted into %s" % (batch, table_size, table))

        finally:
            curs_h2.close()
            curs_mysql.close()

    # Main function
    def export(self):
        print('Creating target MySQL DB...')
        converter.create_new_db()

        print('Obtaining H2 DB info...')
        converter.get_h2_tables()

        for table in converter.h2_tables:
            print('Creating table %s...' % table)
            converter.create_mysql_table(table)

        for table in converter.h2_tables:
            print('Exporting table %s...' % table)
            converter.export_h2_table(table)

        print("H2 DB '%s' successfully exported to MySQL DB '%s'." % (H2_DB_PATH, MYSQL_DB_NAME))


if __name__ == "__main__":
    # These should be passed on class initialization but I'm too lazy to change it now

    # Careful when providing the path to a H2 database.
    # You should provide the full path WITHOUT the extension.
    # https://stackoverflow.com/questions/23403875/how-to-see-all-tables-in-my-h2-database-at-localhost8082/34551665
    H2_DB_PATH = 'path-to-h2-database'

    MYSQL_DB_NAME = 'H2Export'
    MYSQL_DB_HOST = 'localhost'
    MYSQL_DB_USER = 'root'
    MYSQL_DB_PASS = 'pass'

    # Number of entries being read and written at a time
    BATCH_SIZE = 1000

    converter = H2toMySQL()
    # converter.reset_mysql() Unnecessary, for debugging only
    converter.export()

    # To test string escaping
    # print( converter.escape_strings("'")  )
    # print( converter.escape_strings("\"") )
    # print( converter.escape_strings("\\") )
    # print( converter.escape_strings('%')  )
    # print( converter.escape_strings('_')  )


