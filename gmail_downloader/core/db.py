import motor.motor_asyncio
from pymongo import MongoClient

db_obj = {
    'client' : None,
    'database' : ''
}

db_obj_sync = {
    'client' : None,
    'database' : ''
}

def db_connect():
    db_obj['client'] = motor.motor_asyncio.AsyncIOMotorClient()
    db_obj['database'] = db_obj['client']['gmail_database']

    return db_obj['database']

def sync_db_connect():
    db_obj_sync['client'] = MongoClient()
    db_obj_sync['database'] = db_obj_sync['client']['gmail_database']

    return db_obj_sync['database']