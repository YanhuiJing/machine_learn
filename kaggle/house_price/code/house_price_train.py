# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score


def house_price_data_convert():
    """
        对房价预测数据进行预处理
    """
    train_df = pd.read_csv("../data/train.csv", index_col=0)
    test_df = pd.read_csv("../data/test.csv", index_col=0)
    y_train = np.log1p(train_df.pop("SalePrice"))

    all_df = pd.concat((train_df, test_df), axis=0)

    # 将数字特征转换为字符串类型,使用get_dummy_df完成one_hot数据转换
    all_df["MSSubClass"] = all_df["MSSubClass"].astype(str)
    all_dummy_df = pd.get_dummies(all_df)

    # 对数字化特征空数据使用平均值进行填充
    mean_cols = all_dummy_df.mean()
    all_dummy_df = all_dummy_df.fillna(mean_cols)

    # 对数字特征数据进行归一化处理
    numeric_cols = all_dummy_df.columns[all_dummy_df.dtypes != 'object']
    numeric_cols_means = all_dummy_df.loc[:, numeric_cols].mean()
    numeric_cols_std = all_dummy_df.loc[:, numeric_cols].std()

    all_dummy_df.loc[:, numeric_cols] = (all_dummy_df.loc[:, numeric_cols] - numeric_cols_means) / numeric_cols_std

    dummy_train_df = all_dummy_df.loc[train_df.index]
    dummy_test_df = all_dummy_df.loc[test_df.index]

    return dummy_train_df, y_train, dummy_test_df


def house_price_model_cross_ridge(train_data, y_train):
    """
        模型交叉验证,获取最佳参数
    """

    x_train = train_data.values

    alphas = np.logspace(-3, 2, 50)

    test_scores = []

    for alpha in alphas:
        clf = Ridge(alpha)
        test_score = np.sqrt(-cross_val_score(clf, x_train, y_train, cv=10, scoring='neg_mean_squared_error'))
        test_scores.append(np.mean(test_score))

    plt.plot(alphas, test_scores)
    plt.title("Alpha vs CV Error")


def house_price_model_cross_randomforest(train_data, train_y, test_data):
    return ""


if __name__ == '__main__':
    train_data,y_train,test_data = house_price_data_convert()

    # print(np.logspace(-3, 2, 50))
