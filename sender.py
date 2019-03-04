import requests
from utils import transaction, registration


class Sender:
    """
    Instance to send event into particular Google Analytics property
    :param property: str, Google Analytics property ID
    :param debug: boolean, if true, sends hit to the debug url
    """

    def __init__(self, property, debug=False):
        self.property = property
        self.headers = {"User-Agent": "Python Script"}
        if debug:
            self.url = "https://www.google-analytics.com/debug/collect"
        else:
            self.url = "https://www.google-analytics.com/collect"

    def send_event(self, payload):
        """
        Sends hit using measurement protocol. 
        https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide
        """
        r = requests.post(self.url, data=payload, headers=self.headers)
        
        return r