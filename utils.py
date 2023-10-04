import json
import re
from datetime import datetime
from typing import Dict, Union, Generator
import functools
import os
import time


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_t = time.perf_counter()
        f_value = func(*args, **kwargs)
        elapsed_t = time.perf_counter() - start_t
        mins = elapsed_t // 60
        print(
            f"'{func.__name__}' elapsed time: {mins} minutes, {elapsed_t - mins * 60:0.2f} seconds"
        )
        return f_value

    return wrapper_timer


def load_wapo(wapo_jl_path: Union[str, os.PathLike]) -> Generator[Dict, None, None]:
    """
    Unlike HW2, load_wapo should be an iterator in this assignment. It's more memory-efficient when you need to
    load each document and build the inverted index.
    At each time, load_wapo will yield a dictionary of the following format:

    {
        "id": 1,
        "title": "Many Iowans still don't know who they will caucus for",
        "author": "Jason Horowitz",
        "published_date": 2011-12-31 20:37:52,
        "content_str": "Iran announced a nuclear fuel breakthrough and test-fired ..."
      }
    Compared to HW2, you should also make the following changes:
    - exclude any document without a title
    - replace the original value of the key "id" with an integer that corresponds to the order of each document
      that has been loaded. For example. the id of the first yielded document is 0 and the second is 1 and so on.
    - convert the value of "published_date" to a readable format.
      This one is given as follows, so just sure you apply it in your implementation
            %: from datetime import datetime
            %: doc["published_date"] = datetime.fromtimestamp(doc["published_date"] / 1000.0)

    :param wapo_jl_path:
    :return:
    """
    # TODO:
    res = dict()
    with open(wapo_jl_path) as file:
        new_id = 0
        for line in file:
            parsed_json = json.loads(line)
            if parsed_json is None or 'contents' not in parsed_json or parsed_json['contents'] is None:
                continue
            if 'title' not in parsed_json or parsed_json['title'] is None or parsed_json['title'] == "":
                continue
            content_str = ""
            for content in parsed_json['contents']:
                if content is None:
                    continue
                if 'content' in content:
                    raw_content = content['content']
                    processed_content = re.sub('<.*?>', '', str(raw_content))
                    content_str += processed_content + " "
            parsed_json.pop('contents', None)
            parsed_json['content_str'] = content_str
            parsed_json['id'] = new_id

            if 'published_date' in content:
                parsed_json["published_date"] = datetime.fromtimestamp(parsed_json["published_date"] / 1000.0)
            res[new_id] = parsed_json
            new_id += 1
    for content in res:
        #print(res[content])
        yield res[content]


if __name__ == "__main__":
    pass
