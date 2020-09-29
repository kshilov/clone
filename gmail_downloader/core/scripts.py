from .config import *
from json import JSONDecoder
from requests_toolbelt.multipart import decoder
from aiogoogle.models import Response, Request
import re


def decode_multipart(response):
    data = decoder.MultipartDecoder(response.content.encode('utf-8'),
                                    response.headers.get('Content-Type'))
    return data


def get_json(part):
    text = part.text
    i = text.find('{')
    if i == -1:
        raise Exception("Json not found")

    result, index = JSONDecoder().raw_decode(text[i:])

    return result


def parse_ids_from_batch(content):
    items = re.findall("/gmail/v1/users/me/messages/([^?]*)\\?*", content, re.MULTILINE)
    return items


async def create_col_with_index(db, col, field, unique=True):
    await db[col].create_index(field, unique=True)


async def save_page(db, page, col=RAW_DATA):
    result = await db[col].insert_one(page)


async def save_structured(db, data, col=STRUCTURED_DATA):
    try:
        result = await db[col].insert_many(data['messages'])
    except Exception as e:
        print(f"ERROR: can't save _id={data['_id']} error={str(e)}")
        return False

    return True


async def convert_data(db, col=RAW_DATA):
    await create_col_with_index(db, STRUCTURED_DATA, 'id', unique=True)

    data_found = False

    cursor = db[col].find({'status': {'$in': [None, STATUS_NEED_CONVERTION]}})
    async for data in cursor:
        data_found = True
        try:
            res = await save_structured(db, data)
            if res:
                await db[col].update_one({'_id': data['_id']}, {'$set': {'status': STATUS_CONVERTED}})
        except Exception as e:
            print(f"ERROR in a loop: {str(e)}")

    if not data_found:
        print(f"No data found")


async def get_ids(db, total=REQUESTS_PER_BATCH * BATCHES_PER_TIME, col=STRUCTURED_DATA):
    cursor = db[col].find({'status': {'$in': [None, STATUS_READY]}},
                          {'id': 1, '_id': 0}).limit(total)

    res = await cursor.to_list(length=total)

    return res


async def save_raw_messages(db, data, col=RAW_MESSAGE):
    res = await db[col].insert_many(data)
    return res


async def message_status_update(db, _ids, target=STRUCTURED_DATA, source=RAW_MESSAGE):
    cursor = db[source].find({'_id': {'$in': _ids}},
                             {'id': 1, '_id': 0})

    res = await cursor.to_list(length=len(_ids))

    if res:
        msg_ids = []
        for m in res:
            if not m.get('id', None):
                continue
            msg_ids.append(m['id'])
        await db[target].update_many({'id': {'$in': msg_ids}}, {'$set': {'status': STATUS_RAW_SAVED}})


async def put_on_quarantine(db, req, col=STRUCTURED_DATA):
    try:
        msg_ids = parse_ids_from_batch(req.data)
        await db[col].update_many({'id': {'$in': msg_ids}}, {'$set': {'status': STATUS_QUARANTINE}})
    except Exception as e:
        print(f"...ERROR put on quarantine {str(e)}")
        return


async def save_batch_response(db, response):
    if not isinstance(response, Response):
        print(f"...ERROR save_batch_response: wrong response type={type(response)}")
        if isinstance(response, Request):
            await put_on_quarantine(db, response)
        return

    try:
        multipart = decode_multipart(response)
        messages = []
        for part in multipart.parts:
            try:
                m = get_json(part)
                if m and m.get('id', None):
                    messages.append(m)
                elif m.get('error', None):
                    print(f"ERROR for part: {m['error']}")
                else:
                    print(f"Unknown error for part")
            except Exception as e:
                print(f"ERROR: getting json for error={str(e)}")
                continue

        res = await save_raw_messages(db, messages)
        if res:
            await message_status_update(db, res.inserted_ids)
    except Exception as e:
        print(f"ERROR save_batch_response: {str(e)} ")


async def delete_broken(db, ids_col=RAW_DATA, msg_col=RAW_MESSAGE):
    cursor = db[msg_col].find({'id': None})

    res = await cursor.to_list(length=1000000)
    if res:
        print(f"...found {len(res)} messages without ID")
        d = await db[msg_col].delete_many({'id': None})
        print(f"...deleted documents {d}")


async def fix_statuses(db, ids_col=STRUCTURED_DATA, msg_col=RAW_MESSAGE):
    count = await db[msg_col].count_documents({})
    print(f"...found {count} documents in RAW_MESSAGE")

    cursor = db[msg_col].find({}, {'id': 1, '_id': 0})

    res = await cursor.to_list(length=count)

    if res:
        msg_ids = []
        for m in res:
            if not m.get('id', None):
                continue
            msg_ids.append(m['id'])
        await db[ids_col].update_many({'id': {'$in': msg_ids}}, {'$set': {'status': STATUS_RAW_SAVED}})
        print(f"...updated {len(msg_ids)} messages")