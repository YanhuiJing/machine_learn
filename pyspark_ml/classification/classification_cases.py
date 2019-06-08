# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

from pyspark.ml.feature import IndexToString,StringIndexer,VectorIndexer,VectorAssembler

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import ClusteringEvaluator

def getSparkSession():
    spark=SparkSession.builder.\
                    appName("sparkMl").\
                    config("spark.master","local").\
                    getOrCreate()

    return spark


def logstic_regression_usecase():
    """
        maxIter:最大迭代次数
        regPram:正则化强度
        elasticNetParam:用于指定L1和L2正则影响的权重
    """

    spark=getSparkSession()
    training = spark.read.format("libsvm").load("../data/lib_svm.txt")

    #use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    mlrModel = mlr.fit(training)

    # Print the coefficients and intercepts for logistic regression with multinomial family
    print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
    print("Multinomial intercepts: " + str(mlrModel.interceptVector))

    trainingSummary = mlrModel.summary

    # Obtain the objective per iteration
    objectiveHistory = trainingSummary.objectiveHistory
    print("objectiveHistory:")
    for objective in objectiveHistory:
        print(objective)

    # Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    # 获取roc假正率和召回率数据
    trainingSummary.roc.show()
    # 获取roc曲线下方面积
    print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

    # Set the model threshold to maximize F-Measure
    # 获取不通过阈值下的调和平均数
    fMeasure = trainingSummary.fMeasureByThreshold
    fMeasure.show()
    # maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
    # bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']) \
    #     .select('threshold').head()['threshold']
    # mlr.setThreshold(bestThreshold)


def multi_logstic_regression_usecase():
    """
        模型性能检测todo
    """
    spark=getSparkSession()
    training = spark \
        .read \
        .format("libsvm") \
        .load("../data/multi_classification.txt")

    training.select("label").distinct().show()

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,family="multinomial")

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for multinomial logistic regression
    # 打印权重参数和截距
    print("Coefficients: \n" + str(lrModel.coefficientMatrix))
    print("Intercept: " + str(lrModel.interceptVector))

    trainingSummary =lrModel.summary

    # Obtain the objective per iteration
    # 获取模型训练历史数据
    objectiveHistory = trainingSummary.objectiveHistory
    print("objectiveHistory:")
    for objective in objectiveHistory:
        print(objective)


    # for multiclass, we can inspect metrics on a per-label basis
    print("False positive rate by label:")
    for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("True positive rate by label:")
    for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("Precision by label:")
    for i, prec in enumerate(trainingSummary.precisionByLabel):
        print("label %d: %s" % (i, prec))

    print("Recall by label:")
    for i, rec in enumerate(trainingSummary.recallByLabel):
        print("label %d: %s" % (i, rec))

    print("F-measure by label:")
    for i, f in enumerate(trainingSummary.fMeasureByLabel()):
        print("label %d: %s" % (i, f))

    accuracy = trainingSummary.accuracy
    falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    truePositiveRate = trainingSummary.weightedTruePositiveRate
    fMeasure = trainingSummary.weightedFMeasure()
    precision = trainingSummary.weightedPrecision
    recall = trainingSummary.weightedRecall
    print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
          % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))


def decision_tree_usecase():
    spark=getSparkSession()

    data = spark.read.format("libsvm").load("../data/lib_svm.txt")

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "indexedLabel", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[2]
    # summary only
    print(treeModel)


def random_decision_tree_usecase():
    spark=getSparkSession()

    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("../data/lib_svm.txt")

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                   labels=labelIndexer.labels)

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))

    rfModel = model.stages[2]
    print(rfModel)  # summary only


def kmeans_usecase():
    spark=getSparkSession()
    schema = ''
    for i in range(65):
        schema = schema + '_c' + str(i) + ' DOUBLE' + ','
    schema = schema[:len(schema) - 1]
    df_train = spark.read.csv('../data/optdigits.tra', schema=schema)
    df_test = spark.read.csv('../data/optdigits.tes', schema=schema)
    cols = []
    for i in range(65):
        cols.append("_c" + str(i))
    df_train.head = cols
    df_test.head = cols
    assembler = VectorAssembler(inputCols=cols[:-1], outputCol="features")
    train_output = assembler.transform(df_train)
    test_output = assembler.transform(df_test)
    train_features = train_output.select("features").toDF('features')
    test_features = test_output.select("features").toDF('features')
    train_features.show(truncate=False)
    test_features.show(truncate=False)
    kmeans = KMeans().setK(10).setSeed(1)
    model = kmeans.fit(train_features)
    predictions = model.transform(test_features)

    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))



if __name__ == '__main__':

    kmeans_usecase()



