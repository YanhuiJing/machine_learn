#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
    添加隐藏层,提升模型训练准确度
    添加隐藏层,减少模型训练的次数可以快速达到相同的准确度
"""

from __future__ import print_function
import numpy as np

from keras.datasets import mnist
from keras.models import Sequential
from keras.layers.core import Dense,Activation
from keras.optimizers import SGD
from keras.utils import np_utils

#重复性设置
np.random.seed(1671)

#训练轮数,模型基于训练集的重复次数,每次迭代中,优化器尝试调整权重,使目标函数最小化
NB_EPOCH=20

#优化器进行权重更新前要观察的实例数,优化器权重通过小批次的更新回馈更频繁
BATCH_SIZE=128

#日志显示，0为不在标准输出流输出日志信息，1为输出进度条记录，2为每个epoch输出一行记录
VERBOSE=2

#输出个数等于数字个数
NB_CLASSES=10
#优化器类型
OPTIMIZER=SGD()
#隐藏层神经元个数
N_HIDEN=128
#训练集中用作验证集的数据比例
VALIDATION_SPLIT=0.2

(x_train,y_train),(x_test,y_test)=mnist.load_data()

RESHAPED=784

x_train=x_train.reshape(60000,RESHAPED)
x_test=x_test.reshape(10000,RESHAPED)
x_train=x_train.astype('float32')
x_test=x_test.astype('float32')

#数值归一化,每个像素最大亮度为255,使用最大亮度进行数值归一化
x_train /= 255
x_test /= 255

#将类向量转换为二值类别矩阵,例子：1=>[0,1,0,0,0,0,0,0,0,0]
y_train=np_utils.to_categorical(y_train,NB_CLASSES)
y_test=np_utils.to_categorical(y_test,NB_CLASSES)

#序贯模型
model=Sequential()

#添加隐藏层,增加数据每次训练的迭代次数
model.add(Dense(N_HIDEN,input_shape=(RESHAPED,)))
model.add(Activation('relu'))
model.add(Dense(N_HIDEN))
model.add(Activation('relu'))
model.add(Dense(NB_CLASSES))
model.add(Activation('softmax'))
model.summary()


model.compile(loss='categorical_crossentropy',optimizer=OPTIMIZER,metrics=['accuracy'])
history=model.fit(x_train,y_train,batch_size=BATCH_SIZE,epochs=NB_EPOCH,verbose=VERBOSE,validation_split=VALIDATION_SPLIT)

score=model.evaluate(x_test,y_test,verbose=VERBOSE)
print("Test score:",score[0])
print("Test accuracy:",score[1])





