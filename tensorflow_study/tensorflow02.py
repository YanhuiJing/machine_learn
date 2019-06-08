import tensorflow as tf

# """ 创建常量,变量 """
# x = tf.Variable([1, 2])
# a = tf.constant([3, 3])
#
# sub = tf.subtract(x, a)
# add = tf.add(x, a)
#
# """ 初始化全局变量"""
# init = tf.global_variables_initializer()
#
# with tf.Session() as sess:
#     """ 启动初始化变量 """
#     sess.run(init)
#
#     print(sess.run(sub))
#     print(sess.run(add))


state = tf.Variable(0)

new_value = tf.add(state, 1)
""" tensorflow里面的赋值需要使用assign """
update = tf.assign(state, new_value)

init = tf.global_variables_initializer()

with tf.Session() as sess:

    sess.run(init)

    """ 每个op返回的对象是一个tensor,运行tensor需要通过sess.run()来启动"""
    print(sess.run(state))

    """ 循环遍历,不断调用tensor """
    for _ in range(5):
        sess.run(update)
        print(sess.run(state))




