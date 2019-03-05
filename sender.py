import requests

DAYS_DELTA = 30
DATE_FORMAT = "%Y-%m-%d"
REGS_FILE = "regs.csv"
DEBUG = False  # whether hits are sent to debug url

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


if __name__ == "__main__":

    import datetime
    import pandas as pd
    from utils import (transaction, registration, string_date, read_yaml_config,
                       check_query_result, SaveResults, check_user)
    from sql_queries import REGS_QUERY
    from dataloader import Vertica

    # read yaml config
    config = read_yaml_config(config_path="config.yml")
    constants = config["constants"]
    vertica_config = config["vertica"]
    http_ref = constants["STOCKSUP_HTTP_REF"]

    # init dates
    today = datetime.datetime.today()
    yesterday = string_date(today - datetime.timedelta(days=1), format=DATE_FORMAT)
    tdby = string_date(today - datetime.timedelta(days=DAYS_DELTA), format=DATE_FORMAT)  # the day before yesterday or earlier

    # init vertica instance and db connection
    v = Vertica(config=vertica_config)
    v.connect()

    # extract regs
    regs = v.query_vertica(query=REGS_QUERY.format(tdby, yesterday, http_ref))
    if check_query_result(regs):
        regs_columns = ["uid", "cid", "affiliate_id", "utm_campaign", "reg_date", "device"]
        print(regs[0])
        
        # save results and send events
        saver = SaveResults()

        send_adr = Sender(property=constants["UA_ANDROID"], debug=DEBUG)
        send_ios = Sender(property=constants["UA_IOS"], debug=DEBUG)
        send_desk = Sender(property=constants["UA_WEB"], debug=DEBUG)
        
        regs_df = pd.DataFrame(regs, columns=regs_columns)
        uids = set(regs_df["uid"])

        try:
            saved_regs = saver.read_csv(filename=REGS_FILE)
        except FileNotFoundError:
            # write empty file with colnames like regs_df
            regs_df[:0].to_csv(REGS_FILE, index=False)
            saved_regs = saver.read_csv(filename=REGS_FILE)

        for user in uids:
            # check users in user_base

            check = check_user(user_id=user, userbase=saved_regs, current_users=regs_df, userbase_filename=REGS_FILE)

            if check and ("adr_app" in regs_df[regs_df["uid"]==user]["utm_campaign"]):
                event = regs_df[regs_df["uid"]==user]
                event_ts = event["reg_date"].values[0]
                event_ts_delta = (today - event_ts).total_seconds()*1000  # timedelta between event and current moment
                if event_ts_delta > 4*60*60*100:
                    event_ts_delta = 0
                reg_payload = registration(ga_property=constants["UA_ANDROID"], 
                                           client_id=event["cid"], 
                                           event_category=constants["REG_EVENT_CATEGORY"],
                                           event_action=constants["REG_EVENT_ACTION"],
                                           event_label=constants["REG_EVENT_LABEL"],
                                           event_timedelta=int(event_ts_delta),
                                           source=event["affiliate_id"],
                                           campaign=event["utm_campaign"])

                r = send_adr.send_event(payload=reg_payload)  # send hit to Google Analytics
                print(r.status_code)
                print(r.text)
            
            elif check and ("ios_app" in regs_df[regs_df["uid"]==user]["utm_campaign"]):
                event = regs_df[regs_df["uid"]==user]
                event_ts = event["reg_date"].values[0]
                event_ts_delta = (today - event_ts).total_seconds()*1000  # timedelta between event and current moment
                if event_ts_delta > 4*60*60*100:
                    event_ts_delta = 0

                reg_payload = registration(ga_property=constants["UA_IOS"], 
                                           client_id=event["cid"], 
                                           event_category=constants["REG_EVENT_CATEGORY"],
                                           event_action=constants["REG_EVENT_ACTION"],
                                           event_label=constants["REG_EVENT_LABEL"],
                                           event_timedelta=int(event_ts_delta),
                                           source=event["affiliate_id"],
                                           campaign=event["utm_campaign"])

                r = send_ios.send_event(payload=reg_payload)  # send hit to Google Analytics
                print(r.status_code)
                print(r.text)
            
            else:
                event = regs_df[regs_df["uid"]==user]
                event_ts = event["reg_date"].values[0]
                event_ts_delta = (today - event_ts).total_seconds()*1000  # timedelta between event and current moment
                if event_ts_delta > 4*60*60*100:
                    event_ts_delta = 0

                reg_payload = registration(ga_property=constants["UA_WEB"], 
                                           client_id=event["cid"], 
                                           event_category=constants["REG_EVENT_CATEGORY"],
                                           event_action=constants["REG_EVENT_ACTION"],
                                           event_label=constants["REG_EVENT_LABEL"],
                                           event_timedelta=int(event_ts_delta),
                                           source=event["affiliate_id"],
                                           campaign=event["utm_campaign"])

                r = send_desk.send_event(payload=reg_payload)  # send hit to Google Analytics
                print(r.status_code)
                print(r.text)
            
        saver.save_query(regs, columns=regs_columns, filename=REGS_FILE)