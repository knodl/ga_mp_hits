import yaml


def registration(ga_property, client_id, campaign_source, event_category, event_action, 
                 event_timedelta, source, v=1, event_label="None"):
    """
    Creates payload dictionary for a custom event.

    :param v: The Protocol version. The current value is '1'. 
              This will only change when there are changes made that are not backwards compatible.
    :param tid: The tracking ID / web property ID. The format is UA-XXXX-Y. All collected data is associated by this ID.
    :param cid: This anonymously identifies a particular user, device, or browser instance. ClientId.
    :param cd1: Custom dimension (index = 1)
    :param t: Hit type
    :param ec: Specifies the event category. Must not be empty.
    :param ea: Specifies the event action. Must not be empty.
    :param el: Specifies the event label.
    :param qt: The value represents the time delta (in milliseconds) between when the hit being reported occurred 
               and the time the hit was sent. The value must be greater than or equal to 0.
    :param dt: The title of the page / document.
    :param cs: Specifies the campaign source.
    """
    event_payload = dict()
    event_payload["tid"] = ga_property  # UA["UA_WEB"] GA property
    event_payload["cid"] = client_id  # clientId
    event_payload["cd1"] = client_id  # clientId into custom dimension
    event_payload["t"] = "event"  # hit type
    event_payload["ec"] = event_category
    event_payload["ea"] = event_action
    event_payload["el"] = event_label
    event_payload["qt"] = event_timedelta  # time delta (in milliseconds) between when the hit being reported occurred and the time the hit was sent
    event_payload["dt"] = "offline"  # document title (sometimes hits are not sent without this one)
    event_payload["cs"] = source  # traffic source (affiliateId)
    event_payload["v"] = v

    return event_payload


def transaction(ga_property, client_id, campaign_source, transaction_id, revenue, currency, tax,
                 event_timedelta, source, v=1):
    """
    Creates payload dictionary for a transaction event.

    :param v: The Protocol version. The current value is '1'. 
              This will only change when there are changes made that are not backwards compatible.
    :param tid: The tracking ID / web property ID. The format is UA-XXXX-Y. All collected data is associated by this ID.
    :param cid: This anonymously identifies a particular user, device, or browser instance. ClientId.
    :param cd1: Custom dimension (index = 1)
    :param t: Hit type
    :param ti: A unique identifier for the transaction.
    :param tr: Specifies the total revenue associated with the transaction.
    :param tt: Specifies the total tax of the transaction.
    :param cu: When present indicates the local currency for all transaction currency values.
    :param qt: The value represents the time delta (in milliseconds) between when the hit being reported occurred 
               and the time the hit was sent. The value must be greater than or equal to 0.
    :param dt: The title of the page / document.
    :param cs: Specifies the campaign source.
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
    transaction_payload["cs"] = campaign_source

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