# -*- coding:utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np
import urllib
import matplotlib.dates as mdates

def sample01():
    """
        绘制基于点的连线图
    """
    plt.plot([1,2,3,4],[5,7,4,8])
    plt.show()


def sample02():
    """
        设置坐标轴及视图名称
    """
    x1=[1,2,3]
    y1=[5,7,4]

    x2=[1,2,3]
    y2=[10,14,12]

    plt.plot(x1,y1,label="first line")
    plt.plot(x2,y2,label="second line")

    plt.xlabel("x")
    plt.ylabel("y")
    plt.title("view")

    #生成默认图例
    plt.legend()
    plt.show()


def sample03():
    """
        多条数据直方图
    """
    plt.bar([1, 3, 5, 7, 9], [5, 2, 7, 8, 2], label="Example one")

    plt.bar([2, 4, 6, 8, 10], [8, 6, 2, 5, 6], label="Example two", color='g')
    plt.legend()
    plt.xlabel('bar number')
    plt.ylabel('bar height')

    plt.title('Epic Graph\nAnother Line! Whoa')

    plt.show()


def sample04():
    population_ages = [22, 55, 62, 45, 21, 22, 34, 42, 42, 4, 99, 102, 110, 120, 121, 122, 130, 111, 115, 112, 80, 75,
                       65, 54, 44, 43, 42, 48]

    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130]

    plt.hist(population_ages, bins, histtype='bar', rwidth=0.8)

    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Interesting Graph\nCheck it out')
    plt.legend()
    plt.show()


def sample05():
    """
        散点图
    """
    x = [1, 2, 3, 4, 5, 6, 7, 8]
    y = [5, 2, 4, 2, 1, 4, 5, 2]

    plt.scatter(x, y, label='skitscat', color='k', s=25, marker="o")

    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('Interesting Graph\nCheck it out')
    plt.legend()
    plt.show()


def sample06():
    """
        饼图
    """
    slices = [7, 2, 2, 13]
    activities = ['sleeping', 'eating', 'working', 'playing']
    cols = ['c', 'm', 'r', 'b']

    plt.pie(slices,
            labels=activities,
            colors=cols,
            startangle=90,
            shadow=True,
            explode=(0, 0.1, 0, 0),
            autopct='%1.1f%%')

    plt.title('Interesting Graph\nCheck it out')
    plt.show()


def sample07():
    plt.style.use('fivethirtyeight')

    x = np.linspace(0, 10)

    # Fixing random state for reproducibility
    np.random.seed(19680801)

    fig, ax = plt.subplots()

    ax.plot(x, np.sin(x) + x + np.random.randn(50))
    ax.plot(x, np.sin(x) + 0.5 * x + np.random.randn(50))
    ax.plot(x, np.sin(x) + 2 * x + np.random.randn(50))
    ax.plot(x, np.sin(x) - 0.5 * x + np.random.randn(50))
    ax.plot(x, np.sin(x) - 2 * x + np.random.randn(50))
    ax.plot(x, np.sin(x) + np.random.randn(50))
    ax.set_title("'fivethirtyeight' style sheet")

    plt.show()




if __name__ == '__main__':
    sample07()