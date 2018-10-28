# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import Tokenizer,HashingTF,IDF
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import PolynomialExpansion
from pyspark.ml.feature import StandardScaler,MinMaxScaler,MaxAbsScaler
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorIndexer

def getSparkSession():
    spark=SparkSession.builder.\
                    appName("sparkMl").\
                    config("spark.master","local").\
                    getOrCreate()

    return spark


def tf_idf_usecase():
    spark=getSparkSession()
    sentenceData = spark.createDataFrame([
        (0.0, "Hi I heard about Spark"),
        (0.0, "I wish Java could use case classes"),
        (1.0, "Logistic regression models are neat")
    ], ["label", "sentence"])
    """
        Tokenizer:分词器
    """
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    wordsData = tokenizer.transform(sentenceData)
    wordsData.show(truncate=False)

    """
        HashinfTF:将words列的所有文本转换为词袋进行表示
    """
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures",numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("words","rawFeatures").show(truncate=False)

    """
        TF-IDF:TF=>单词在单篇文档中出现的频率
               IDF=>(文档数+1)/(出现单词的文档数+1)取对数,文档数是固定的,
                    单词出现的文档数越多,说明该词的重要性越低
    """
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("label", "features").show(truncate=False)


def word2vec_usecase():
    """
        word2vec模型解释:todo
    """
    spark=getSparkSession()
    documentDF = spark.createDataFrame([
        ("Hi I heard about Spark".split(" "),),
        ("I wish Java could use case classes".split(" "),),
        ("Logistic regression models are neat".split(" "),)
    ], ["text"])

    # Learn a mapping from words to Vectors.
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
    model = word2Vec.fit(documentDF)

    result = model.transform(documentDF)
    result.show(truncate=False)


def count_vectorizer_usecase():
    spark=getSparkSession()
    df = spark.createDataFrame([
        (0, "a b".split(" ")),
        (1, "a b b c a".split(" "))
    ], ["id", "words"])

    """
        vocabSize=>指定字典的大小 
        minDF=>指定最少的文档数目
    """
    cv = CountVectorizer(inputCol="words", outputCol="features")

    model = cv.fit(df)

    result = model.transform(df)
    result.show(truncate=False)


def polynomial_expansion_usecase():
    """
        多项式扩展数据特征
    """
    spark=getSparkSession()

    df = spark.createDataFrame([
        (Vectors.dense([2.0, 1.0]),),
        (Vectors.dense([0.0, 0.0]),),
        (Vectors.dense([3.0, -1.0]),)
    ], ["features"])

    polyExpansion = PolynomialExpansion(degree=3, inputCol="features", outputCol="polyFeatures")
    polyDF = polyExpansion.transform(df)

    polyDF.show(truncate=False)

def standard_scaler_usecase():
    """
        数据归一化的三种的方式:标准方差化，最大值绝对值化，minmax化
    """
    spark=getSparkSession()
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])

    # scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    # scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

    scalerModel = scaler.fit(dataFrame)

    scaledData = scalerModel.transform(dataFrame)
    scaledData.select("features", "scaledFeatures").show(truncate=False)


def sql_transformer_usecase():
    """
        通过sql方式实现对数据特征的转换
        "_THIS_" 代表的是输入数据对应的dataset
    """
    spark=getSparkSession()
    df = spark.createDataFrame([
        (0, 1.0, 3.0),
        (2, 2.0, 5.0)
    ], ["id", "v1", "v2"])
    sqlTrans = SQLTransformer(
        statement="SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
    sqlTrans.transform(df).show(truncate=False)


def vector_assembler_usecase():
    """
        将给定列中的list数据组合到一个单独的list中
    """
    spark=getSparkSession()
    dataset = spark.createDataFrame(
        [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
        ["id", "hour", "mobile", "userFeatures", "clicked"])

    assembler = VectorAssembler(
        inputCols=["hour", "mobile", "userFeatures"],
        outputCol="features")

    output = assembler.transform(dataset)
    output.select("features", "clicked").show(truncate=False)


def imputer_usecase():
    """
        用于计算数据集中的缺失值,使用指定的策略进行数据填充，
        strategy指定数据填充策略,
    """
    spark=getSparkSession()
    df = spark.createDataFrame([
        (1.0, float("nan")),
        (2.0, float("nan")),
        (float("nan"), 3.0),
        (4.0, 4.0),
        (5.0, 5.0)
    ], ["a", "b"])

    imputer = Imputer(inputCols=["a", "b"], outputCols=["out_a", "out_b"])
    model = imputer.fit(df)

    model.transform(df).show()


def onehot_encoder_usecase():
    spark=getSparkSession()
    df = spark.createDataFrame([
        (0.0, 1.0),
        (1.0, 0.0),
        (2.0, 1.0),
        (0.0, 2.0),
        (0.0, 1.0),
        (2.0, 0.0)
    ], ["categoryIndex1", "categoryIndex2"])

    encoder = OneHotEncoder(inputCol="categoryIndex2",outputCol="categoryVec2")
    encoded = encoder.transform(df)
    encoded.show()


def vector_indexer_usecase():
    spark=getSparkSession()

    data = spark.read.format("libsvm").load("data/lib_svm.txt")

    data.show(1)

    indexer = VectorIndexer(inputCol="features", outputCol="indexed", maxCategories=10)
    indexerModel = indexer.fit(data)

    categoricalFeatures = indexerModel.categoryMaps
    print("Chose %d categorical features: %s" %
          (len(categoricalFeatures), ", ".join(str(k) for k in categoricalFeatures.keys())))

    # Create new column "indexed" with categorical values transformed to indices
    indexedData = indexerModel.transform(data)
    indexedData.show(1,truncate=False)

if __name__ == '__main__':
    vector_indexer_usecase()