import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import numpy as np
"""
    模型优化:
        1,代价函数=>使用交叉熵替代算数平方和
        2,过拟合=>dropout 
"""

# 载入数据
mnist = input_data.read_data_sets("MNIST_data", one_hot=True)

# 每个批次的大小,神经网络训练每次放入100张图片进行训练
batch_size = 100

n_batch = mnist.train.num_examples // batch_size

# 定义两个palceholder

x = tf.placeholder(tf.float32, [None, 784])
y = tf.placeholder(tf.float32, [None, 10])

# 定义一个简单神经网络
W = tf.Variable(tf.zeros([784, 10]))
b = tf.Variable(tf.zeros([10]))
prediction = tf.nn.softmax(tf.matmul(x, W) + b)

""" 二次代价函数优化 """
# loss = tf.reduce_mean(tf.square(y - prediction))

""" 交叉熵代价函数 """
loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y, logits=prediction))

# 使用梯度下降法
train_step = tf.train.GradientDescentOptimizer(0.2).minimize(loss)

init = tf.global_variables_initializer()

# 结果存放在一个布尔型列表中,argmax返回同一张列表中最大值所在的位置
correct_prection = tf.equal(tf.argmax(y, 1), tf.argmax(prediction, 1))

accuracy = tf.reduce_mean(tf.cast(correct_prection, tf.float32))

with tf.Session() as sess:
    sess.run(init)

    for epoch in range(20):
        for batch in range(n_batch):
            batch_xs, batch_ys = mnist.train.next_batch(batch_size)
            sess.run(train_step, feed_dict={x: batch_xs, y: batch_ys})

        acc = sess.run(accuracy, feed_dict={x: mnist.test.images, y: mnist.test.labels})

        print("{}, accuarcy:{}".format(epoch, acc))
