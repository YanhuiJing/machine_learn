import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

x_data = np.linspace(-1, 1, 200)[:, np.newaxis]
noise = np.random.normal(0, 0.02, x_data.shape)

y_data = np.square(x_data) + noise

""" 定义占位符,定义输入数据[None,1]表示单列数据任意行"""
x = tf.placeholder(tf.float32, [None, 1])
y = tf.placeholder(tf.float32, [None, 1])

""" 定义神经网络中间层,网络中间层的选择是设置训练的纬度(这个可以自定义),[1,10]表示10个权重值,"""
Weight_L1 = tf.Variable(tf.random_normal([1, 10]))
biases_L1 = tf.Variable(tf.zeros([1, 10]))
Wx_plus_b_l1 = tf.matmul(x, Weight_L1) + biases_L1
L1 = tf.nn.tanh(Wx_plus_b_l1)

""" 定义神经网络中间层,[1,10]*[10,5] 输出结果是[1,5],因此截距调整是[1,5]"""
Weight_L2 = tf.Variable(tf.random_normal([10, 5]))
biases_L2 = tf.Variable(tf.zeros([1, 5]))
Wx_plus_b_l2 = tf.matmul(L1, Weight_L2) + biases_L2
L2 = tf.nn.tanh(Wx_plus_b_l2)


""" 定义神经网络输出层"""
Weight_L3 = tf.Variable(tf.random_normal([5, 1]))
biases_L3 = tf.Variable(tf.zeros([1, 1]))

Wx_plus_b_l3 = tf.matmul(L2, Weight_L3) + biases_L3

prediction = tf.nn.tanh(Wx_plus_b_l3)

# 二次代价函数

loss = tf.reduce_mean(tf.square(y - prediction))

train_step = tf.train.GradientDescentOptimizer(0.1).minimize(loss)

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())

    for _ in range(100):
        sess.run(train_step, feed_dict={x: x_data, y: y_data})

    """ 调用最后训练的模型进行数据预测 """
    prediction_value = sess.run(prediction, feed_dict={x: x_data})

    plt.figure()
    plt.scatter(x_data, y_data)

    plt.plot(x_data, prediction_value, 'r-', lw=5)
    plt.show()
