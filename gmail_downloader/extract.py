import pymongo
from .core.db import sync_db_connect
import re
import pickle
import pandas as pd
import os

mail_regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))

RAW_DATA = 'raw_message'
READY = 1
ENRICHED = 2
FAILED = -1
EXTRACTED = 3
FILENAME_PANDAS = "./pandas_emails.pkl"
FILENAME_CSV = "./pandas_emails.csv"

data = {
    'from' : {},
    'to' : {},
    'cc' : {},
    'bcc' : {},
    'subject' : {},
    'reply-to' : {}
}

skip = ['subject']

DATA_KEYS = dict(map(lambda i: (i[0], True), data.items()))

def get_enriched(db, limit):
    res = db[RAW_DATA].find({'status' : ENRICHED}, DATA_KEYS).limit(limit)
    return res

def add_email(key, emails, subject):
    for email in emails:
        if not data[key].get(email):
            data[key][email] = 1
        else:
            data[key][email] += 1

        if not data['subject'].get(email):
            data['subject'][email] = subject

def categorize(messages):
    for m in messages:
        for k,v in m.items():
            if k in skip:
                continue

            if k in data.keys():
                if not isinstance(v, list):
                    v = [v]
                add_email(k, v, m.get('subject', ''))

def main():
    db = sync_db_connect()

    print("..get_enriched started")
    res = get_enriched(db, limit=500000)

    print("...categorize started")
    categorize(res)

    data_df = pd.DataFrame(data)

    print("...saving data")
    if not os.path.exists(FILENAME_PANDAS):
        data_df.to_pickle(FILENAME_PANDAS)
    else:
        print(f"...{FILENAME_PANDAS} already exists")

    print("...creating csv")
    if not os.path.exists(FILENAME_CSV):
        data_df.to_csv(FILENAME_CSV)
    else:
        print(f"...{FILENAME_CSV} already exists")

    print("Success")

if __name__ == '__main__':
    main()