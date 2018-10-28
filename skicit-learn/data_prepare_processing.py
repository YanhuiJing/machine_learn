# -*- coding:utf-8 -*-

import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import Binarizer
from sklearn.preprocessing import Imputer
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import OneHotEncoder

from sklearn.feature_extraction import DictVectorizer




"""
    StandardScaler:通过标准化,缩小特征之间的数量级差异
"""

def standardscaler_usecase():
    X_train = np.array([[1., -1., 2.],
                        [2., 0., 0.],
                        [0., 1., -1.]])

    scaler=StandardScaler()
    scaler.fit(X_train)

    print(scaler.mean_)
    print(scaler.scale_)
    print(scaler.transform(X_train))

"""
    mixmax特征缩放:(data-min)/(max-min)=>将值缩放到0～1之间
"""
def max_min_scaler_usecase():
    X_train = np.array([[1., -1., 2.],
                        [2., 0., 0.],
                        [0., 1., -1.]])

    scaler=MinMaxScaler()
    scaler.fit(X_train)

    print(scaler.data_max_)
    print(scaler.data_min_)
    print(scaler.scale_)
    print(scaler.transform(X_train))

"""
    maxabsscaler=>data/abs(max) 将数据特征缩放到-1到1之间
"""
def max_abs_scaler_usecase():
    X_train = np.array([[1., -1., 2.],
                        [2., 0., 0.],
                        [0., 1., -1.]])

    scaler=MaxAbsScaler()
    scaler.fit(X_train)

    print(scaler.max_abs_)
    print(scaler.scale_)
    print(scaler.transform(X_train))

"""
    normalize:归一化,可以选择L1、L2范式
    L1范式
    L2范式
"""
def normalize_usecase():
    X_train = np.array([[1., -1., 2.],
                        [2., 0., 0.],
                        [0., 1., -1.]])
    normalize=Normalizer(norm="l1")
    normalize.fit(X_train)
    X_L1_normalize=normalize.transform(X_train)
    print(X_L1_normalize)

    normalize=Normalizer(norm="l2")
    normalize.fit(X_train)
    X_L2_normalize=normalize.transform(X_train)
    print(X_L2_normalize)

"""
    特征二值化,定义指定值,大于该值为1,小与该值为0
"""
def binarizer_usecase():
    X_train = np.array([[1., -1., 2.],
                        [2., 0., 0.],
                        [0., 1., -1.]])

    binarizer=Binarizer(threshold=1.1)
    binarizer.fit(X_train)
    print(binarizer.transform(X_train))

"""
    缺失值差补,将数据中的nan值补充为0或者其他值
"""

def imputer_usecase():
    imp = Imputer(missing_values='NaN', strategy='mean', axis=0)
    imp.fit([[1, 2], [np.nan, 3], [7, 6]])
    Imputer(axis=0, copy=True, missing_values='NaN', strategy='mean', verbose=0)
    X = [[np.nan, 2], [6, np.nan], [7, 6]]
    print(imp.transform(X))


"""
    生成多项式特征,某些模型数据特征缺乏,通过多项式增加数据特征的丰富度
    interaction_only=True =>
    X的特征已经从 (X_1, X_2, X_3) 转换为 (1, X_1, X_2, X_3, X_1X_2, X_1X_3, X_2X_3, X_1X_2X_3) 。
"""
def polynomialFeatures_usecase():
    X = np.arange(9).reshape(3, 3)
    poly = PolynomialFeatures(degree=3, interaction_only=True)
    print(poly.fit_transform(X))

"""
    OneHotEncoder使用 m 个可能值转换为 m 值化特征，将分类特征的每个元素转化为一个值。
"""
def one_hot_encoder_usecase():
    enc =OneHotEncoder(n_values=[2, 3, 4])
    enc.fit([[1, 2, 3], [0, 2, 0]])
    print(enc.transform([[1, 0, 0]]).toarray())





if __name__ == '__main__':
    one_hot_encoder_usecase()



