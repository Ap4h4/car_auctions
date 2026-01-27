import datetime
from pymongo import MongoClient
from secrets_folder.secrets2 import MONGO_USER, MONGO_PASSWORD, CLUSTER_NAME
from urllib.parse import quote_plus #uses to escape special characters in secrets
import logging

logger = logging.getLogger(__name__)


def connect_to_db():
    #dynamically assiging username and password won't work without escapting special characters
    username = quote_plus(MONGO_USER)
    password = quote_plus(MONGO_PASSWORD)
    uri = f"mongodb+srv://{username}:{password}@db1.f0f9oa3.mongodb.net/?appName={CLUSTER_NAME}"
    client = MongoClient(uri, ssl=True)
    my_db = client.get_database("debt_auctions")
    logger.info("Connected to db "+ CLUSTER_NAME +" at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return my_db

"""
INSERT FUNCTIONS
"""
def mongo_bulk_upload(collection_name, df):
    records = df.to_dict(orient="records")
    db = connect_to_db()
    collection = db[collection_name]
    collection.insert_many(records)
    logger.info("Inserted to db "+ CLUSTER_NAME +" at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


"""
DELETE FUNCTIONS
"""
def mongo_truncate_collection(collection_name):
    db = connect_to_db()
    collection = db[collection_name]
    collection.delete_many({})
    logger.info("Truncated "+ CLUSTER_NAME +" at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

"""
READ FUNCTIONS
"""
def get_all_auctions():
    """
    Docstring for get_all_auctions
    :return:
    """
    db=connect_to_db()
    collection = db["auctions"]
    #details = collection.find({},{"auction_item": 1, "_id": 1})
    details = collection.find({})
    auctions_dict = {}
    fields = [
    "auction_item",
    "auction_date",
    "auction_city",
    "auction_region",
    "starting_price",
    "target_price",
    "auction_link",
    "item_ts"
    ]
    for document in details:
        doc_id = str(document["_id"])
        values = [document.get(f) for f in fields]
        auctions_dict[doc_id] = values
    length = len(auctions_dict)
    logger.info("Get all auctions " + str(length) + " from Mongo at "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return auctions_dict