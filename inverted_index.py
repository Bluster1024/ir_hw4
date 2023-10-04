import heapq
from collections import defaultdict
from math import sqrt
from typing import List, Tuple, Dict, Iterable
from collections import Counter

import pymongo

from pa4.utils import timer
from pa4.text_processing import TextProcessing
from mongo_db import insert_vs_index, insert_doc_len_index, query_doc, query_vs_index, query_doc_len_index, \
    get_term_dict, insert_term_dict, get_documents_counts

text_processor = TextProcessing.from_nltk()

client = pymongo.MongoClient("localhost", 27017)
db = client["vs_ir_2023_wapo"]


def get_doc_vec_norm(term_tfs: List[float]) -> float:
    """
    helper function, should be called in build_inverted_index
    compute the length of a document vector
    :param term_tfs: a list of term weights (log tf) for one document
    :return:
    """
    res = sqrt(sum([x ** 2 for x in term_tfs]))
    return res


@timer
def build_inverted_index(
        wapo_docs: Iterable,
) -> None:
    """
    load wapo_pa4.jl to build two indices:
        - "vs_index": for each normalized term as a key, the value should be a list of tuples; each tuple stores the doc id this term appear in and the term weight (log tf)
        - "doc_len_index": for each doc id as a key, the value should be the "length" of that document vector
    insert the indices by using mongo_db.insert_vs_index and mongo_db.insert_doc_len_index method
    """
    # TODO:
    term_vs_index = defaultdict(list)
    doc_len_index = dict()
    stop_words = text_processor.STOP_WORDS

    for doc in wapo_docs:
        doc_id = doc["id"]
        doc_text = doc["content_str"]
        doc_title = doc["title"]
        normalized_tokens = text_processor.get_normalized_tokens(doc_title, doc_text)
        token_counter = Counter(normalized_tokens)
        for token in token_counter:
            if token not in stop_words:
                term_vs_index[token].append([doc_id, text_processor.tf(token_counter[token])])

        term_weights = [text_processor.tf(token_counter[token]) for token in token_counter]
        doc_len = get_doc_vec_norm(term_weights)
        doc_len_index[doc_id] = doc_len
    index_list = list()
    for term in term_vs_index:
        index_list.append({"term": term, "term_tf": term_vs_index[term]})
    insert_vs_index(index_list)
    doc_len_list = list()
    for doc_id in doc_len_index:
        doc_len_list.append({"doc_id": doc_id, "length": doc_len_index[doc_id]})
    insert_doc_len_index(doc_len_list)


def parse_query(query: str) -> Tuple[List[str], List[str], List[str]]:
    """
    helper function, should be called in query_inverted_index
    given each query, return a list of normalized terms, a list of stop words and a list of unknown words separately
    """
    # TODO:
    normalized_terms_list = list()
    unknown_words_list = list()
    normalized_tokens = text_processor.get_normalized_tokens('', query)
    for token in normalized_tokens:
        if query_vs_index(token) is None:
            if token not in text_processor.STOP_WORDS:
                print('unknown word: ', token)
                unknown_words_list.append(token)
        else:
            normalized_terms_list.append(token)
    stop_words_list = list(text_processor.get_stop_words(query))
    return normalized_terms_list, stop_words_list, unknown_words_list


def top_k_docs(doc_scores: Dict[int, float], k: int) -> List[Tuple[float, int]]:
    """
    helper function, should be called in query_inverted_index method
    given the doc_scores, return top k doc ids and corresponding scores using a heap
    :param doc_scores: a dictionary where doc id is the key and cosine similarity score is the value
    :param k:
    :return: a list of tuples, each tuple contains (score, doc_id)
    """
    # Create an empty heap
    heap = []

    # Iterate over the doc_scores dictionary
    for doc_id, score in doc_scores.items():
        # Push the (score, doc_id) tuple onto the heap
        heapq.heappush(heap, (score, doc_id))

        # If the size of the heap exceeds k, pop the smallest item
        if len(heap) > k:
            heapq.heappop(heap)

    # Return the k largest (score, doc_id) tuples in descending order of score
    top_k = sorted(heap, reverse=True)

    return top_k


def query_inverted_index(
        query: str, k: int
) -> Tuple[List[Tuple[float, int]], List[str], List[str]]:
    """
    disjunctive query over the vs_index with the help of mongo_db.query_vs_index,
    mongo_db.query_doc_len_index methods return a list of matched documents (output
    from the function top_k_docs), a list of stop words and a list of unknown words separately
    """
    # TODO:
    normalized_terms_list, stop_words_list, unknown_words_list = parse_query(query)
    # doc_scores: a dictionary where doc id is the key and cosine similarity score is the value
    doc_scores = dict()
    query_tokens = text_processor.get_normalized_tokens('', query)
    query_tokens_counter = Counter(query_tokens)
    query_key_list = list(query_tokens_counter.keys())
    query_value_dict_dict = defaultdict(dict)


    for i in range(len(query_key_list)):
        token = query_key_list[i]
        N_query = len(query_tokens)
        df_query = None
        vs_index_result = query_vs_index(token)
        if vs_index_result is not None:
            df_query = len(vs_index_result['term_tf'])
            for pair in vs_index_result['term_tf']:
                doc_id = pair[0]
                tf = pair[1]
                query_value_dict_dict[doc_id][token] = text_processor.tf(tf) * text_processor.idf(N_query, df_query)

    docID_docVal_dict_dict = defaultdict(dict)
    for token in normalized_terms_list:
        term_vs_index = query_vs_index(token)
        if term_vs_index is None:
            continue
        term = term_vs_index["term"]
        docid_tf = term_vs_index["term_tf"]
        if docid_tf is not None:
            for pair in docid_tf:
                doc_id = pair[0]
                tf = pair[1]
                query_result = query_doc(doc_id)
                if query_result is None:
                    continue
                # N is the number of documents in the collection
                N = get_documents_counts()
                df = len(term_vs_index["term_tf"])
                if term in query_key_list:
                    docID_docVal_dict_dict[doc_id][term] = tf * text_processor.idf(N, df)

    for doc_id in docID_docVal_dict_dict:
        doc_val_dict = docID_docVal_dict_dict[doc_id]
        doc_len = query_doc_len_index(doc_id)["length"]
        query_value_dict = query_value_dict_dict[doc_id]
        doc_scores[doc_id] = text_processor.cosine_similarity(query_key_list, query_value_dict, doc_val_dict, doc_len)
    top_k = top_k_docs(doc_scores, k)
    return top_k, stop_words_list, unknown_words_list


def query_words_in_doc(doc_id: int, query: str) -> List[str]:
    """
    given a doc_id and a query string, return a list of normalized words in the query string that
    appear in the document
    """
    normalized_terms_list, stop_words_list, unknown_words_list = parse_query(query)

    res = list()
    for token in normalized_terms_list:
        term_vs_index = query_vs_index(token)
        if term_vs_index is None:
            continue
        docid_tf = term_vs_index["term_tf"]
        if docid_tf is not None:
            for pair in docid_tf:
                if doc_id == pair[0]:
                    res.append(token)
    return res


def query_words_idf(query: str) -> List[Tuple[str, float]]:
    """
    return a List where the key is a normalized word and the value is the idf of the word
    """
    normalized_terms_list, stop_words_list, unknown_words_list = parse_query(query)
    res = list()
    for token in normalized_terms_list:
        term_vs_index = query_vs_index(token)
        if term_vs_index is None:
            res.append((token, 0))
            continue
        N = get_documents_counts()
        df = len(term_vs_index["term_tf"])
        res.append((token, text_processor.idf(N, df)))
    return res


if __name__ == "__main__":
    pass
