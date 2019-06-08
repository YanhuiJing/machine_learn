# -*- coding:utf-8 -*-

from sklearn.datasets import load_iris
from sklearn.datasets import *
import numpy as np
from sklearn.model_selection import train_test_split


def get_modeldata_iris():
    iris = load_iris()

    x, y = iris.data, iris.target

    # random_state=>随机种子设置;stratify=>指定分类的列,
    # 指定后获取的随机数据目标值符合总体的分布规律,如果不指定训练集与测试集各分类的分布与总体不一致,影响模型训练效果
    train_x, test_x, train_y, test_y = train_test_split(x, y,
                                                        train_size=0.7,
                                                        test_size=0.3,
                                                        random_state=1,
                                                        stratify=y)

    print("All",np.bincount(y)/float(len(y))*100)
    print("training", np.bincount(train_y) / float(len(train_y)) * 100)
    print("testing", np.bincount(test_y)/float(len(test_y))*100)


    return train_x, train_y, test_x, test_y

if __name__ == '__main__':

    data=load_boston()

    print(data.keys())
    print(data.DESCR)




