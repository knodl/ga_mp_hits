import vertica_python
import numpy as np
import sqlite3

# todo: create base class and inherit vertica and sqlite
# classes from this base class


class Vertica(object):
    """
    Executes queries from vertica.

    Parameters:
    ----------
    config, dict
        config to initiate vertica connection
    """

    conn = None

    def __init__(self, config):
        self.config = config

    def connect(self):
        self.conn = vertica_python.connect(**self.config)

    def return_cursor(self):
        """
        Checks whether there is any db connection. If there is one then
        current cursor is used. Id there is no active connection
        then a new cursor object is being created.
        """

        if self.conn is not None:
            cursor = self.conn.cursor()
        else:
            self.connect()
            cursor = self.conn.cursor()

        return cursor

    def query_vertica(self, query):
        """
        Queries vertica. Returns list of lists as query result.
        :param query: SQL string
        """
        cursor = self.return_cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        result = np.array(result)
        return result


class DBSaver:
    """
    Saves pandas dataframe into sqlite table.

    Parameters:
    ----------
    db_name string,
        name of database to interact with
    """

    conn = None

    def __init__(self, db_name):
        self.db_name = db_name

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def return_cursor(self):
        """
        Checks whether there is any db connection. If there is one then
        current cursor is used. Id there is no active connection
        then a new cursor object is being created.
        """

        if self.conn is not None:
            cursor = self.conn.cursor()
        else:
            self.connect()
            cursor = self.conn.cursor()

        return cursor

    def select_query(self, query):
        """
        Queries sqlite. Returns list of lists as query result.
        :param query: SQL string
        """
        cursor = self.return_cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        result = np.array(result)
        return result

    def other_query(self, query):
        """
        Queries sqlite. Returns list of lists as query result.
        :param query: SQL string
        """
        cursor = self.return_cursor()
        cursor.execute(query)
        self.conn.commit()


if __name__ == "__main__":
    # create test sqlite db
    sqlt = DBSaver("test.db")
    sqlt.query_sqlite(query="""CREATE TABLE albums
                  (title text, artist text, release_date text,
                   publisher text, media_type text)
               """)
