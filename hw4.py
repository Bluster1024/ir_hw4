from pathlib import Path
import argparse
from flask import Flask, render_template, request, session
from pa4.utils import load_wapo
from pa4.inverted_index import build_inverted_index, query_inverted_index, query_words_in_doc, query_words_idf
from pa4.mongo_db import db, insert_docs, query_doc


app = Flask(__name__)
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

data_dir = Path(__file__).parent.joinpath("pa4_data")
wapo_path = data_dir.joinpath("wapo_pa4.jl")
#wapo_path = data_dir.joinpath("bigtest.jl")

#build_inverted_index(load_wapo(wapo_path))
if not "wapo_docs" in db.list_collection_names():
    insert_docs(load_wapo(wapo_path))


# home page
@app.route("/")
def home():
    return render_template("home.html")


# result page
@app.route("/results", methods=["POST"])
def results():
    # TODO:
    # TODO:
    query_text = request.form["query"]  # Get the raw user query from home page
    matched_docs, stop_words, unknown_words = query_inverted_index(query_text, 50)
    matched_docs_files = list()
    matched_ids = list()
    id_score_dict = dict()
    query_words_idf_list = query_words_idf(query_text)
    for score, doc_id in matched_docs:
        doc = query_doc(doc_id)
        doc["score"] = round(score,4)
        matched_query_words = query_words_in_doc(doc_id, query_text)
        doc["matched_query_words"] = matched_query_words
        matched_docs_files.append(doc)
        matched_ids.append(doc_id)
        id_score_dict[doc['id']] = round(score,4)
    session['matched_docs_files_id'] = matched_ids
    session['stop_words'] = stop_words
    session['unknown_words'] = unknown_words
    session['id_score_dict'] = id_score_dict
    page_id = 1
    match_counts = len(matched_docs)
    session['match_counts'] = match_counts
    is_button_visible = len(matched_docs) > 8 * page_id
    return render_template("results.html", query_text=query_text,
                           this_page=matched_docs_files[:8 * page_id], page_id=page_id,
                           is_button_visible=is_button_visible, stop_words=stop_words,
                           unknown_words=unknown_words, match_counts=match_counts,
                           query_words_idf_list=query_words_idf_list)  # add variables as you wish


# "next page" to show more results
@app.route("/results/<int:page_id>", methods=["POST"])
def next_page(page_id):
    # TODO:
    id_score_dict = session['id_score_dict']
    query_text = request.form["query_text"]  # Get the raw user query from home page
    matched_docs_files_id = session['matched_docs_files_id']
    query_words_idf_list = query_words_idf(query_text)
    matched_docs_files = [query_doc(doc_id) for doc_id in matched_docs_files_id]
    for doc in matched_docs_files:
        doc["score"] = id_score_dict[str(doc["id"])]
        matched_query_words = query_words_in_doc(doc["id"], query_text)
        doc["matched_query_words"] = matched_query_words
    match_counts = session['match_counts']
    stop_words = session['stop_words']
    unknown_words = session['unknown_words']
    is_button_visible = match_counts > 8 * page_id
    return render_template("results.html", query_text=query_text,
                           this_page=matched_docs_files[8 * (page_id - 1):8 * page_id], page_id=page_id,
                           is_button_visible=is_button_visible, stop_words=stop_words,
                           unknown_words=unknown_words, match_counts=match_counts,
                           query_words_idf_list=query_words_idf_list)  # add variables as you wish


# document page
@app.route("/doc_data/<int:doc_id>")
def doc_data(doc_id):
    # TODO:
    doc = query_doc(doc_id)
    return render_template("doc.html", title=doc["title"], content=doc["content_str"], author=doc["author"],
                           date=doc["published_date"])


if __name__ == "__main__":


    parser = argparse.ArgumentParser(description="VS IR system")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()

    if args.build:
        build_inverted_index(load_wapo(wapo_path))

    if args.run:
        app.run(debug=True, port=5000)
    #app.run(debug=True, port=5000)
