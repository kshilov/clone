import pymongo
from .core.db import sync_db_connect
import re

mail_regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))

RAW_DATA = 'raw_message'
READY = 1
ENRICHED = 2
FAILED = -1

def restore_failed(db):
    res = db[RAW_DATA].update({'status' : FAILED}, {'$set' : {'status' : READY}})
    print(f"...restored {res}")

def stats(db):
    total = db[RAW_DATA].find({'status' : ENRICHED}, {'status' : True})
    print(f"...STATS: found {len(list(total))} enriched documents")

def get_ready_data(db, limit):
    res = db[RAW_DATA].find({'status' : {'$in' : [None, READY]}}).limit(limit)
    print(f"...found messages to enrich limit={limit}")

    return res

def parse_email(data):
    res = re.findall(mail_regex, data.lower())
    if not res:
        print(f"..Can't extract email for {data}")
        return [data.lower()]

    res = [x[0] for x in res]
    return res

def extract_features(message):
    features = {}

    payload = message.get('payload', None)
    if not payload:
        raise Exception(f"...No payload for _id={message['_id']}")

    headers = payload.get('headers', None)
    if not headers:
        raise Exception(f"...No headers for _id={message['_id']}")

    for header in headers:
        name = header['name'].lower()
        val = header['value']
        if name == 'subject':
            features[name] = val
            continue

        elif name in ['from', 'to', 'cc', 'bcc', 'reply-to']:
            email = parse_email(val)
            features[name] = email
            continue

    return features

def construct_update(features, status = ENRICHED):
    res = { 'status' : status}

    for k, v in features.items():
        res[k] = v

    return res

def enrich_messages(db, messages):
    total = 0
    updated = {
        'matched_count' : 0,
        'modified_count' : 0
    }
    for message in messages:
        try:
            features = extract_features(message)

            data = construct_update(features)

            total = total + 1
            res = db[RAW_DATA].update_one({'_id' : message['_id']}, {'$set' : data})
            if res:
                updated['matched_count'] = updated['matched_count'] + res.matched_count
                updated['modified_count'] = updated['modified_count'] + res.modified_count
        except Exception as e:
            print(str(e))
            continue
    print("...FINSISHED")
    print(f"...tried update_one call={total}")
    print(f"...finished update_one call={updated}")


def main():
    db = sync_db_connect()
    restore_failed(db)

    messages = get_ready_data(db, 100000)
    if not messages:
        print("...Didn't find any messages")
        exit(0)

    enrich_messages(db, messages)

if __name__ == '__main__':
    main()