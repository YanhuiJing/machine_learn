#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
    使用卷积神经网络对数字识别进行训练
"""
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
from keras import backend as k
from keras.models import Sequential
from keras.layers.convolutional import Convolution2D,Conv2D
from keras.layers.convolutional import MaxPooling2D
from keras.layers.core import Activation,Flatten,Dense
from keras.datasets import mnist
from keras.utils import np_utils
from keras.optimizers import SGD,RMSprop,Adam
import numpy as np
import matplotlib.pyplot as plt

class LeNet:

    @staticmethod
    def build(input_shape,classes):
        model=Sequential()
        #滤波器的设置,滤波器不改变
        model.add(Convolution2D(20,kernel_size=5,padding="same",input_shape=input_shape))
        model.add(Activation("relu"))
        #池化的设置,池化操作改变
        model.add(MaxPooling2D(pool_size=(2,2),strides=(2,2)))

        model.add(Conv2D(50,kernel_size=5,padding="same"))
        model.add(Activation("relu"))
        model.add(MaxPooling2D(pool_size=(2,2),strides=(2,2)))

        model.add(Flatten())
        model.add(Dense(500))

        model.add(Activation("relu"))
        model.add(Dense(classes))
        model.add(Activation("softmax"))
        return model

NB_EPOCH=1
BATCH_SIZE=128
VERBOSE=1
OPTIMIZER=Adam()
VALIDATION_SPLIT=0.2
IMG_ROWS,IMG_COLS=28,28
NB_CLASSES=10

INPUT_SHAPE=(1,IMG_ROWS,IMG_COLS)
(x_train,y_train),(x_test,y_test)=mnist.load_data()

k.set_image_dim_ordering("th")
x_train=x_train.astype("float32")
x_test=x_test.astype("float32")

x_train /= 255
x_test /= 255

x_train=x_train[:,np.newaxis,:,:]
x_test=x_test[:,np.newaxis,:,:]

y_train=np_utils.to_categorical(y_train,NB_CLASSES)
y_test=np_utils.to_categorical(y_test,NB_CLASSES)

model=LeNet.build(input_shape=INPUT_SHAPE,classes=NB_CLASSES)
#模型汇总,展示模型每层神经训练的详细数据
model.summary()


model.compile(loss='categorical_crossentropy',optimizer=OPTIMIZER,metrics=['accuracy'])
history=model.fit(x_train,y_train,batch_size=BATCH_SIZE,epochs=NB_EPOCH,verbose=VERBOSE,validation_split=VALIDATION_SPLIT)

score=model.evaluate(x_test,y_test,verbose=VERBOSE)
print("Test score:",score[0])
print("Test accuracy:",score[1])