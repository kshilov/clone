import asyncio
from aiogoogle import Aiogoogle
from aiogoogle.models import Request, Response
from core.config import *
from core.oauth import get_service, convert_user_creds, convert_client_creds, get_token
from core.gmail import *
from core.db import db_connect
from core.scripts import *
import time
import uvloop

USER_CREDS = None
CLIENT_CREDS = None
PAGE_TOKEN = None

async def show_stats(db):
    count = 1000000

    res = db[STRUCTURED_DATA].find({'status': STATUS_RAW_SAVED})
    total = await res.to_list(length=count)
    print(f"....FINSIHED with STATUS_RAW_SAVED={len(total)}")

    broken = db[STRUCTURED_DATA].find({'status': STATUS_QUARANTINE})
    total = await broken.to_list(length=count)
    print(f"....FINSIHED with STATUS_QUARANTINE={len(total)}")


async def remove_quarantene(db):
    await db[STRUCTURED_DATA].update_many({'status': STATUS_QUARANTINE}, {'$set': {'status': STATUS_READY}})

async def on_init(db):
    await remove_quarantene(db)

    await delete_broken(db)

    await fix_statuses(db)


async def main():
    if not USER_CREDS:
        raise Exception(f"ERROR: user_creds={USER_CREDS}, call get_token first")

    db = db_connect()

    await on_init(db)

    service = await get_service()

    while True:
        try:
            start_time = time.time()

            ids = await get_ids(db)

            print(f"...{i} Batch request ids count={len(ids)}")

            req_list = get_batch_requests_list(service, ids)

            req_tasks = [
                get_batch_response(Aiogoogle(user_creds=USER_CREDS, client_creds=CLIENT_CREDS), r) for r in req_list
            ]

            resp_list = await asyncio.gather(*req_tasks, return_exceptions=True)

            resp_tasks = [
                save_batch_response(db, r) for r in resp_list
            ]

            await asyncio.gather(*resp_tasks, return_exceptions=True)

            await show_stats(db)

            exec_time = time.time() - start_time
            print(f"...pause. Execution time: {exec_time}")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"...Error {str(e)}")
            continue

    print("...Finished...")


if __name__ == '__main__':
    USER_CREDS = get_token()
    USER_CREDS = convert_user_creds(USER_CREDS)

    CLIENT_CREDS = convert_client_creds('./credentials.json')

    print("....Installing uvloop....")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    print("....Starting asyncio loop....")
    asyncio.run(main())