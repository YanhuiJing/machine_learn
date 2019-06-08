import tensorflow as tf
""" 了解tensorflow的基本概念,op,tensor,session"""

""" 创建常量op,构建计算图 """

m1 = tf.constant([[3, 3]])
m2 = tf.constant([[2], [3]])

res = tf.matmul(m1, m2)

""" 定义会话,通过会话触发图计算中得op,会话是一个项资源,启动完成之后需要关闭 """
# sees = tf.Session()
# product = sees.run(res)
#
# print(product)
#
# sees.close()

""" 通过with方式创建session,资源会自动关闭 """
with tf.Session() as sees:
    product = sees.run(res)
    print(product)



