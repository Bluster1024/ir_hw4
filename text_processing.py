import re
from math import log
from typing import Any, List, Set

from nltk.tokenize import word_tokenize  # type: ignore
from nltk.stem.porter import PorterStemmer  # type: ignore
from nltk.corpus import stopwords  # type: ignore


class TextProcessing:
    def __init__(self, stemmer, stop_words, *args):
        """
        class TextProcessing is used to tokenize and normalize tokens that will be further used to build inverted index.
        :param stemmer:
        :param stop_words:
        :param args:
        """
        self.stemmer = stemmer
        self.STOP_WORDS = stop_words

    @classmethod
    def from_nltk(
            cls,
            stemmer: Any = PorterStemmer().stem,
            stop_words: List[str] = stopwords.words("english"),
    ) -> "TextProcessing":
        """
        initialize from nltk
        :param stemmer:
        :param stop_words:
        :return:
        """
        return cls(stemmer, set(stop_words))

    def normalize(self, token: str) -> str:
        """
        normalize the token based on:
        1. make all characters in the token to lower case
        2. remove any characters from the token other than alphanumeric characters and dash ("-")
        3. if the processed token after step 2 appears in the stop words list or its length is 1, return an empty string;
           if the processed token after step 2 is NOT in the stop words list and its length is greater than 1, return the stem of the token
        :param token:
        :return:
        """
        # TODO:
        token = token.lower()
        token = re.sub(r'[^a-z0-9-]', '', token)
        if token in self.STOP_WORDS or len(token) == 1:
            return ''
        else:
            return self.stemmer(token)

    def get_normalized_tokens(self, title: str, content: str) -> List[str]:
        """
        pass in the title and content_str of each document, and return a list of normalized tokens (exclude the empty string)
        you may want to apply word_tokenize first to get un-normalized tokens first.
        Note that you don't want to remove duplicate tokens as what you did in HW3, because it will later be used to compute term frequency
        :param title:
        :param content:
        :return:
        """
        # TODO:
        res = list()
        for token in word_tokenize(title):
            if self.normalize(token) != '':
                res.append(self.normalize(token))
        for token in word_tokenize(content):
            if self.normalize(token) != '':
                res.append(self.normalize(token))
        return res

    def get_stop_words(self, s) -> Set[str]:
        res = set()
        for token in word_tokenize(s):
            if token in self.STOP_WORDS:
                res.add(token)
        return res
        pass

    @staticmethod
    def idf(N: int, df: int) -> float:
        """
        compute the logarithmic (base 2) idf score
        :param N: document count N
        :param df: document frequency
        :return:
        """
        # TODO:
        return log(N / df, 2) + 1

    @staticmethod
    def tf(freq: int) -> float:
        """
        compute the logarithmic tf (base 2) score
        :param freq: raw term frequency
        :return:
        """
        # TODO:
        return log(freq, 2) + 1

    def cosine_similarity(self, query_key_list, query_value_dict, doc_val_dict, vector_length):
        """
        compute the cosine similarity between the query and the document
        """
        res = 0
        doc_val_list = list()
        query_value_list = list()
        for i in range(len(query_key_list)):
            if query_key_list[i] in doc_val_dict:
                query_value_list.append(query_value_dict[query_key_list[i]])
            else:
                query_value_list.append(0)
        for i in range(len(query_key_list)):
            if query_key_list[i] in doc_val_dict:
                doc_val_list.append(doc_val_dict[query_key_list[i]])
            else:
                doc_val_list.append(0)
        for i in range(len(query_value_list)):
            res += query_value_list[i] * doc_val_list[i]
        query_vec_length = self.norm(query_value_list)
        if query_vec_length == 0:
            return 0
        res = res / (query_vec_length * vector_length)
        return res
        pass

    def norm(self, query_value_list):
        res = 0
        for i in range(len(query_value_list)):
            res += query_value_list[i] ** 2
        res = res ** 0.5
        return res
        pass



if __name__ == "__main__":
    pass
