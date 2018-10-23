# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import Imputer
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier


df = pd.read_excel("./data/titanic3.xls")

labels = df["survived"].values
features = df[['pclass', 'sex', 'age', 'sibsp', 'parch', 'fare', 'embarked']]

"""
    get_dummies()方法默认将所有单个特征的离散变量转为多个特征
    特征转换,pclass系统默认为连续变量,如果需要对pclass特征转换为离散分类特征,在columns中添加特征列
"""
feature_dummies = pd.get_dummies(features, columns=['pclass', 'sex', 'embarked'])

data = feature_dummies.values

train_data, test_data, train_label, test_lable = train_test_split(data, labels, train_size=0.8, test_size=0.2,
                                                                  random_state=0)
"""
    Imputer是缺失值处理器,自动将nan数据替换为0
"""
imp = Imputer()

imp.fit(train_data)

train_data_finite = imp.transform(train_data)

test_data_finite = imp.transform(test_data)

clf=DummyClassifier("most_frequent")
clf.fit(train_data_finite,train_label)

print(clf.score(test_data_finite,test_lable))

lr=LogisticRegression()
lr.fit(train_data_finite,train_label)
print(lr.score(test_data_finite,test_lable))

rf=RandomForestClassifier()
rf.fit(train_data_finite,train_label)
print(rf.score(test_data_finite,test_lable))





