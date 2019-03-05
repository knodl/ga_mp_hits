import datetime
import yaml
import pandas as pd


def registration(ga_property, client_id, event_category, event_action, 
                 event_timedelta, source, campaign, v=1, event_label="None"):
    """
    Creates payload dictionary for a custom event.

    :param v: The Protocol version. The current value is '1'. 
              This will only change when there are changes made that are not backwards compatible.
    :param ga_property: The tracking ID / web property ID. The format is UA-XXXX-Y. All collected data is associated by this ID.
    :param client_id: This anonymously identifies a particular user, device, or browser instance. ClientId.
    :param event_category: Specifies the event category. Must not be empty.
    :param event_action: Specifies the event action. Must not be empty.
    :param event_label: Specifies the event label.
    :param event_timedelta: The value represents the time delta (in milliseconds) between when the hit being reported occurred 
                            and the time the hit was sent. The value must be greater than or equal to 0.
    :param source: Specifies the campaign source.
    :param campaign: Specifies the campaign name.
    """
    event_payload = dict()
    event_payload["tid"] = ga_property
    event_payload["cid"] = client_id
    event_payload["cd1"] = client_id
    event_payload["t"] = "event"
    event_payload["ec"] = event_category
    event_payload["ea"] = event_action
    event_payload["el"] = event_label
    event_payload["qt"] = event_timedelta
    event_payload["dt"] = "offline"
    event_payload["cs"] = source  # (affiliateId)
    event_payload["cn"] = campaign
    event_payload["v"] = v

    return event_payload


def transaction(ga_property, client_id, source, campaign, transaction_id, revenue, 
                currency, tax, event_timedelta, v=1):
    """
    Creates payload dictionary for a transaction event.

    :param v: The Protocol version. The current value is '1'. 
              This will only change when there are changes made that are not backwards compatible.
    :param ga_property: The tracking ID / web property ID. The format is UA-XXXX-Y. All collected data is associated by this ID.
    :param client_id: This anonymously identifies a particular user, device, or browser instance. ClientId.
    :param transaction_id: A unique identifier for the transaction.
    :param revenue: Specifies the total revenue associated with the transaction.
    :param tax: Specifies the total tax of the transaction.
    :param currency: When present indicates the local currency for all transaction currency values.
    :param event_timedelta: The value represents the time delta (in milliseconds) between when the hit being reported occurred 
                            and the time the hit was sent. The value must be greater than or equal to 0.
    :param source: Specifies the campaign source.
    :param campaign: Specifies the campaign name.
    """
    transaction_payload = dict()
    transaction_payload["tid"] = ga_property
    transaction_payload["cid"] = client_id  # clientId
    transaction_payload["cd1"] = client_id  # clientId into custom dimension
    transaction_payload["t"] = "transaction"  # hit type
    transaction_payload["ti"] = transaction_id  # transaction id
    transaction_payload["tr"] = revenue  # transaction revenue
    transaction_payload["tt"] = tax  # transaction tax
    transaction_payload["cu"] = currency  # transaction currency
    transaction_payload["qt"] = event_timedelta  # time delta (in milliseconds) between when the hit being reported occurred and the time the hit was sent
    transaction_payload["dt"] = "offline"  # document title (sometimes hits are not sent without this one)
    transaction_payload["cs"] = source
    transaction_payload["cn"] = campaign

    return transaction_payload


def read_yaml_config(config_path):
    """
    reads config from yaml file
    """
    with open(config_path, "r") as config_file:
        try:
            config = yaml.load(config_file)
            return config
        except yaml.YAMLError:
            raise Exception


def string_date(dt, format):
    """
    Makes string of particular format from datetime object.
    :param dt: datetime object
    :param format: format of result string output
    """

    return datetime.datetime.strftime(dt, format)


def check_query_result(result):
    """
    Checks whether database returned valid results.
    :param result: sql SELECT query result
    """
    if result.size == 0:
        return False
        raise Exception("The query result is empty. Try check query parameters.")
    else:
        return True


def check_user(user_id, userbase, current_users, userbase_filename):
    """ 
    Checks whether user had this event before. Looks into particular userbase 
    :param user_id: string, user_id to check
    :param userbase: pandas dataframe, dataframe with registered users
    :param current_users: pandas dataframe, dataframe with ready to chek users
    :param userbase_filename: string, filename for file with initial userbase
    """
    user_ids = set(userbase["uid"])
    if user_id in user_ids:
        return False
    else:
        userbase.append(current_users[current_users["uid"]==user_id])
        userbase.to_csv(userbase_filename, index=False)
        return True


class SaveResults:

    def __init__(self):
        pass

    def save_query(self, result, columns, filename):
        """
        Saves query result as csv.
        :param result: numpy.ndarray, query result
        :param columns: list, columns names for pandas dataframe
        :param filename: str, name of result csv file
        """
        df = pd.DataFrame(result, columns=columns)
        df.to_csv(filename, index=False)  # doesn't save index column
        return True

    def read_csv(self, filename):
        """
        Reads csv file into pandas dataframe.
        :param filename: str, name of result csv file
        """
        return pd.read_csv(filename)
