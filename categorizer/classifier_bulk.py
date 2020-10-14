from pprint import pprint
from .db import sync_db_connect

db = sync_db_connect()
class_dict = {
    '0': 'sales',
    '1': 'contractor',
    '2': 'promo',
    '3': 'nletter',
    '4': 'partner',
    '5': 'notification',
    '6': 'personal',
    '7': 'skip',
    '8': 'count',
    '9': 'exit'
}
classes = ['sales', 'contractor', 'promo', 'nletter', 'partner', 'notification', 'personal', '1-skip', '2-count', '3-exit']


def find_by_subject(data):
    res = db['raw_message'].find({'subject' :
        {
            '$regex' : f'^{data}.*',
            '$options' : 'si'
        }
    })
    return list(res)

def classify_ids(ids, label):
    res = db['raw_message'].update_many({'_id' : {'$in' : ids}}, {'$set' : {'label' : label}})
    print(f"..labeled: {res}")

def count_labels():
    res = db['raw_message'].aggregate([
        {"$match" : {'label' : {"$ne" : None}}},
        {"$group" : {'_id':"$label", 'count':{'$sum':1}}}])
    return list(res)


if __name__ == '__main__':
    while(True):
        try:
            subject = str(input("..input subject:"))

            messages = find_by_subject(subject)
            if not messages:
                print("...Not found")
            else:
                ids = [m['_id'] for m in messages]
                print(f"...Found count:{len(ids)}")
                print("...Sample")
                print(f"_id: {messages[0]['_id']}")
                print(f"from: {messages[0]['from']}")
                print(f"to: {messages[0]['to']}")
                print(f"subject: {messages[0]['subject']}")
                print(f"snippet: {messages[0]['snippet']}")
                print(".....")

                pprint(class_dict)
                q = str(input("..input class:"))
                if class_dict[q] == 'skip':
                    continue
                elif class_dict[q] == 'count':
                    res = count_labels()
                    pprint(res)
                    continue
                elif class_dict[q] == 'exit':
                    exit(0)
                elif class_dict[q] in classes:
                    classify_ids(ids, class_dict[q])


        except Exception as e:
            print(f"!!!!ERROR: {str(e)}")
            continue