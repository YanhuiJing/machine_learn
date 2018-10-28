# -*- coding:utf-8 -*-

from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

"""
    将单个特征的多个离散变量转换为多个特征二维变量
"""
def dict_vectorizer():
    dict01 = [
        {"city": "a", "temperatire": 10},
        {"city": "a", "temperatire": 10},
        {"city": "b", "temperatire": 20},
        {"city": "c", "temperatire": 30},
    ]

    vec = DictVectorizer()
    print(vec.fit_transform(dict01).toarray())

    print(vec.get_feature_names())

def count_vectorizer_usercase():
    x=["some say the world end in fire",
       "some say in ice"]

    vectorizer=CountVectorizer()
    vectorizer.fit(x)

    print(vectorizer.vocabulary_)

    x_bag_of_words=vectorizer.transform(x)

    print(x_bag_of_words)

    print(x_bag_of_words.shape)

    print(x_bag_of_words.toarray())

    print(vectorizer.get_feature_names())

    print(vectorizer.inverse_transform(x_bag_of_words))

def tfidf_vectorizer_usecase():
    x=["some say the world end in fire",
       "some say in ice"]

    tfidfvertor=TfidfVectorizer()

    tfidfvertor.fit(x)

    print(tfidfvertor.vocabulary_)

    print(tfidfvertor.transform(x).toarray())

if __name__ == '__main__':
    tfidf_vectorizer_usecase()








