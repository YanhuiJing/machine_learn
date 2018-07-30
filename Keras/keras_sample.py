#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from __future__ import print_function
import numpy as np

from keras.datasets import mnist
from keras.models import Sequential
from keras.layers.core import Dense,Activation
from keras.optimizers import SGD
from keras.utils import np_utils

#重复性设置
np.random.seed(1671)

NB_EPOCH=200
BATCH_SIZE=128
VERBOSE=1
NB_CLASSES=10
OPTIMIZER=SGD()
N_HIDEN=128
VALIDATION_SPLIT=0.2

(x_train,y_train),(x_test,y_test)=mnist.load_data()

RESHAPED=784

x_train=x_train.reshape(60000,RESHAPED)
x_test=x_test.reshape(10000,RESHAPED)
x_train=x_train.astype('float32')
x_test=x_test.astype('float32')

x_train /= 255
x_test /= 255

y_train=np_utils.to_categorical(y_train,NB_CLASSES)
y_test=np_utils.to_categorical(y_test,NB_CLASSES)

model=Sequential()
model.add(Dense(NB_CLASSES,input_shape=(RESHAPED,)))
model.add(Activation('softmax'))
model.summary()

model.compile(loss='categorical_crossentropy',optimizer=OPTIMIZER,metrics=['accuracy'])
history=model.fit(x_train,y_train,batch_size=BATCH_SIZE,epochs=NB_EPOCH,verbose=VERBOSE,validation_split=VALIDATION_SPLIT)

score=model.evaluate(x_test,y_test,verbose=VERBOSE)
print("Test score:",score[0])
print("Test accuracy:",score[1])





