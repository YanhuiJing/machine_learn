import tensorflow as tf
import numpy as np

""" 机器学习要素,变量,损失函数,优化器,最小化损失函数"""

""" 使用numpy生成100个随机点 """

x_data = np.random.rand(100)
y_data = x_data * 0.1 + 0.2

""" 构建一个线性方程组 """
b = tf.Variable(0.0)
k = tf.Variable(0.0)

y = k * x_data + b

""" 二次代价函数 """
loss = tf.reduce_mean(tf.square(y_data - y))

""" 定义一个梯度下降法来进行训练优化器 """
optimizer = tf.train.GradientDescentOptimizer(0.2)

""" 最小化代价函数 """

train = optimizer.minimize(loss)

init = tf.global_variables_initializer()

with tf.Session() as sess:
    sess.run(init)

    for step in range(200):
        sess.run(train)

        if step % 20 == 0:
            print("{},{}".format(step, sess.run([k, b])))
