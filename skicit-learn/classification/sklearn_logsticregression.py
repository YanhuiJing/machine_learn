# -*- coding:utf-8 -*-

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import numpy as np

def get_model_data():
    iris = load_iris()

    x, y = iris.data, iris.target

    train_x, test_x, train_y, test_y = train_test_split(x, y,
                                                        train_size=0.7,
                                                        test_size=0.3,
                                                        random_state=1,
                                                        stratify=y)

    return train_x, train_y, test_x, test_y

def train_model(train_data,train_target,test_data,test_target):
    classifer=LogisticRegression()

    classifer.fit(train_data,train_target)

    """
        训练好的模型可以直接通过score方法获取模型预测的准确率
    """
    # prediction=classifer.predict(test_data)
    # print(prediction)
    # print(test_target)
    # print(np.mean(prediction==test_y))

    print(classifer.score(test_data,test_target))



if __name__ == '__main__':
    # train_x, train_y, test_x, test_y=get_model_data()
    # train_model(train_x, train_y, test_x, test_y)

    ary=np.array([1,2,3,4,5])

    print(np.std(ary))
    print((ary-np.mean(ary))/np.std(ary))
