from pprint import pprint
from .db import sync_db_connect

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
db = sync_db_connect()


def get_next_message():
    message = db['raw_message'].find_one({'label' : None, 'skip': None})
    return message

def skip_message(msg):
    res = db['raw_message'].update_one({'_id' : msg['_id']}, {'$set' : {'skip' : 1}})
    print(f"..skipped: {res}")

def classify_message(msg, label):
    res = db['raw_message'].update_one({'_id' : msg['_id']}, {'$set' : {'label' : label}})
    print(f"..labeled: {res}")

def count_labels():
    res = db['raw_message'].aggregate([
        {"$match" : {'label' : {"$ne" : None}}},
        {"$group" : {'_id':"$label", 'count':{'$sum':1}}}])
    return list(res)


if __name__ == '__main__':
    while(True):
        try:
            message = get_next_message()

            print(".....\n\n\n")
            print(f"_id: {message['_id']}")
            print(f"from: {message['from']}")
            print(f"to: {message['to']}")
            print(f"subject: {message['subject']}")
            print(f"snippet: {message['snippet']}")
            print(".....")

            pprint(class_dict)
            q=str(input("..what's the class:"))
            if class_dict[q] == 'skip':
                skip_message(message)
                print('skipped')

                continue
            elif class_dict[q] == 'count':
                res = count_labels()
                pprint(res)

                continue
            elif class_dict[q] == 'exit':
                exit(0)
            elif class_dict[q] in classes:
                classify_message(message, class_dict[q])

        except Exception as e:
            print(f"!!!!ERROR: {str(e)}")
            continue