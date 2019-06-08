import tensorflow as tf

# Fetch

# input1 = tf.constant(3.0)
# input2 = tf.constant(4.0)
# input3 = tf.constant(5.0)
#
# add = tf.add(input1, input2)
# mul = tf.multiply(input1, add)
#
# with tf.Session() as sess:
#     """ fetch概念是指同时运行多个op """
#     res = sess.run([add, mul])
#     print(res)


# Feed
# 创建占位符

input1 = tf.placeholder(tf.float32)
input2 = tf.placeholder(tf.float32)

output = tf.multiply(input1, input2)

with tf.Session() as sess:
    # feed数据依字典的形式传入,变量数值按照列表的方式意义对应传入
    print(sess.run(output, feed_dict={input1: [8., 3.0], input2: [2.0, 4.0]}))
