import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import numpy as np

"""
    模型优化:
        1,代价函数优化=>使用交叉熵替代算数平方和
        2,防止过拟合:a,增加数据集;2,正则化方法(减小模型复杂度);3,Dropout
        3,优化器:tf.train包下包含各种优化器
            a,标准梯度下降法:先计算所有样本汇总误差,然后根据总误差来更新权值
            b,随机梯度下降法:随机抽取一个样本来计算误差,然后更新权值
            c,折中方案,从总样本中选取一个批次,然后计算这个batch的总误差,根据总误差来更新权值
            
"""

# 载入数据
mnist = input_data.read_data_sets("MNIST_data", one_hot=True)

# 每个批次的大小,神经网络训练每次放入100张图片进行训练
batch_size = 100

n_batch = mnist.train.num_examples // batch_size

# 定义两个palceholder

x = tf.placeholder(tf.float32, [None, 784])
y = tf.placeholder(tf.float32, [None, 10])
keep_prob = tf.placeholder(tf.float32)
""" 定义学习率变量,模型训练过程中改变学习率 """
lr = tf.Variable(0.001, dtype=tf.float32)

# 定义一个简单神经网络
# W = tf.Variable(tf.zeros([784, 10]))
# b = tf.Variable(tf.zeros([10]))
# prediction = tf.nn.softmax(tf.matmul(x, W) + b)

""" 参数初始化优化 """
w1 = tf.Variable(tf.truncated_normal([784, 500], stddev=0.1))
b1 = tf.Variable(tf.zeros([500]) + 0.1)
l1 = tf.nn.tanh(tf.matmul(x, w1) + b1)
""" dropout防过拟合 """
l1_drop = tf.nn.dropout(l1, keep_prob)

w2 = tf.Variable(tf.truncated_normal([500, 300], stddev=0.1))
b2 = tf.Variable(tf.zeros([300]) + 0.1)
l2 = tf.nn.tanh(tf.matmul(l1_drop, w2) + b2)
l2_drop = tf.nn.dropout(l2, keep_prob)

w3 = tf.Variable(tf.truncated_normal([300, 10], stddev=0.1))
b3 = tf.Variable(tf.zeros([10]) + 0.1)
prediction = tf.nn.softmax(tf.matmul(l2_drop, w3) + b3)

""" 二次代价函数优化 """
# loss = tf.reduce_mean(tf.square(y - prediction))

""" 交叉熵代价函数 """
loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y, logits=prediction))

# 使用梯度下降法
""" 使用不同的优化器,优化模型训练 """
# train_step = tf.train.GradientDescentOptimizer(0.2).minimize(loss)
train_step = tf.train.AdamOptimizer(lr).minimize(loss)

init = tf.global_variables_initializer()

# 结果存放在一个布尔型列表中,argmax返回同一张列表中最大值所在的位置
correct_prection = tf.equal(tf.argmax(y, 1), tf.argmax(prediction, 1))

accuracy = tf.reduce_mean(tf.cast(correct_prection, tf.float32))

with tf.Session() as sess:
    sess.run(init)

    for epoch in range(51):

        """ 根据模型批次训练改变学习率曲线,刚开始学习率陡峭,随着批次改变学习率变小"""
        sess.run(tf.assign(lr, 0.001 * (0.95 ** epoch)))
        for batch in range(n_batch):
            batch_xs, batch_ys = mnist.train.next_batch(batch_size)
            sess.run(train_step, feed_dict={x: batch_xs, y: batch_ys, keep_prob: 1})

        acc = sess.run(accuracy, feed_dict={x: mnist.test.images, y: mnist.test.labels, keep_prob: 1})

        print("{}, accuarcy:{}".format(epoch, acc))
