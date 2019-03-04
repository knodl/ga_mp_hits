import vertica_python
import numpy as np


class Vertica(object):
    """
    Executes queries from vertica. Extracts rfm data, for each user from rfm data additionally extracts 
    timestamp of the last deal, timestamp of the last demo deal and timestamp of the last session.
    """

    conn = None

    def __init__(self, config):
        self.config = config

    def connect(self):
        self.conn = vertica_python.connect(**self.config)

    def query_vertica(self, query):
        """
        Queries vertica. Returns list of lists as query result.
        :param query: SQL string
        """

        if self.conn is not None:
            cursor = self.conn.cursor()
        else:
            self.connect()
            cursor = self.conn.cursor()

        cursor.execute(query)
        result = cursor.fetchall()
        result = np.array(result)
        return result
