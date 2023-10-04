from typing import Dict, List, Iterable

import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client["vs_ir_2023_wapo"]


def insert_docs(docs: Iterable) -> None:
    """
    - create a collection called "wapo_docs"
    - add a unique ascending index on the key "id"
    - insert documents into the "wapo_docs" collection
    :param docs: WAPO docs iterator (utils.load_wapo(...))
    :return:
    """
    # TODO:
    collection = None
    if "wapo_docs" not in db.list_collection_names():
        collection = db.create_collection("wapo_docs")
    else:
        collection = db["wapo_docs"]

    # Create a unique ascending index on the key "id"
    collection.create_index([("id", 1)], unique=True)

    # Insert documents into the "wapo_docs" collection
    collection.insert_many(docs)


def insert_vs_index(index_list: List[Dict]) -> None:
    """
    - create a collection called "vs_index"
    - add a unique ascending index on the key "term"
    - insert posting lists (index_list) into the "inverted_index" collection
    :param index_list: posting lists in the format of [{"term": "earlier", "term_tf": [[0, 1], [4, 1], ...]}, ...], the term_tf is a list of [doc_id, tf]
    :return:
    """
    # TODO:
    collection = None
    if "vs_index" not in db.list_collection_names():
        collection = db.create_collection("vs_index")
    else:
        collection = db["vs_index"]
    # create index on the key "term", term is a string
    collection.create_index([("term", 1)], unique=True)
    try:
        collection.insert_many(index_list)
        print("Documents inserted successfully!")
    except Exception as e:
        print("Error while inserting documents:", e)


def insert_doc_len_index(index_list: List[Dict]) -> None:
    """
    - create a collection called "doc_len_index"
    - add a unique ascending index on the key "doc_id"
    - insert list of document vector length (index_list) into the "doc_len_index" collection
    :param index_list: document vector length list in the format of [{"doc_id": 0, "length": 39.53}, ...]
    :return:
    """
    # TODO:
    collection = None
    if "doc_len_index" not in db.list_collection_names():
        collection = db.create_collection("doc_len_index")
    else:
        collection = db["doc_len_index"]
    # create index on the key "doc_id"
    collection.create_index([("doc_id", 1)], unique=True)
    try:
        collection.insert_many(index_list)
        print("Documents inserted successfully!")
    except Exception as e:
        print("Error while inserting documents:", e)


def insert_term_dict(term_dict: Dict) -> None:
    """
    - create a collection called "term_dict"
    - insert term_dict into the "term_dict" collection
    :param term_dict: term_dict in the format of {"earlier": 0, "earliest": 1, ...}
    :return:
    """
    collection = None
    if "term_dict" not in db.list_collection_names():
        collection = db.create_collection("term_dict")
    else:
        collection = db["term_dict"]
    collection.insert_one(term_dict)


def get_term_dict() -> Dict:
    """
    - get term_dict from the "term_dict" collection
    :return: term_dict in the format of {"earlier": 0, "earliest": 1, ...}
    """
    collection = db["term_dict"]
    return collection.find_one()


def query_doc(doc_id: int) -> Dict:
    """
    query the document from "wapo_docs" collection based on the doc_id
    :param doc_id:
    :return:
    """
    # TODO:
    collection = db["wapo_docs"]
    res = collection.find_one({"id": doc_id})
    return res


def query_vs_index(term: str) -> Dict:
    """
    query the vs_index collection by term
    :param term:
    :return: posting lists in the format of [{"term": "earlier", "term_tf": [[0, 1], [4, 1], ...]}, ...] ,
    the term_tf is a list of [doc_id, tf]
    """
    # TODO:
    collection = db["vs_index"]
    res = collection.find_one({"term": term})
    return res


def query_doc_len_index(doc_id: int) -> Dict:
    """
    query the doc_len_index by doc_id
    :param doc_id:
    :return:
    """
    # TODO:
    collection = db["doc_len_index"]
    res = collection.find_one({"doc_id": doc_id})
    return res


def get_documents_counts() -> int:
    """
    get the number of documents in the "wapo_docs" collection
    :return:
    """
    res = db["wapo_docs"].count_documents({})
    return res


if __name__ == "__main__":
    pass
