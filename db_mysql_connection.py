import MySQLdb
class dbsql:

    dbc = ("localhost","root","redhat","events")

    def __init__(self):
        try:
            self.db = MySQLdb.connect(*self.dbc)
            self.cursor = self.db.cursor()

        except:
            print("Cannot Connected to a Mysql server")

    def query_in(self,sql):
        try:
            self.cursor.execute(sql)

        except (MySQLdb.OperationalError,MySQLdb.ProgrammingError) as e:
            raise e

        self.db.autocommit(on=True)





    def close(self):
        self.cursor.close()