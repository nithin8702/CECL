#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:58:46 2019

@author: nithin
"""
# https://docs.databricks.com/applications/machine-learning/mllib/binary-classification-mllib-pipelines.html
# https://medium.com/@dhiraj.p.rai/logistic-regression-in-spark-ml-8a95b5f5434c
# https://datascience-enthusiast.com/Python/PySpark_ML_with_Text_part1.html
# https://datascience-enthusiast.com/Python/ROC_Precision-Recall.html

import pandas as pd
from IPython.display import display # Allows the use of display() for DataFrames
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark = SparkSession.builder.appName('abc').getOrCreate()

dataset = spark.read.option('header', True).option('inferSchema', True).csv("./train.csv")
dataset = dataset.withColumnRenamed("default.payment.next.month", "default_payment_next_month")
dataset = dataset.drop('ID')


dataset_size=float(dataset.select("default_payment_next_month").count())
numPositives=dataset.select("default_payment_next_month").where("default_payment_next_month == 1").count()
per_ones=(float(numPositives)/float(dataset_size))*100
numNegatives=float(dataset_size-numPositives)
print('The number of ones are {}'.format(numPositives))
print('Percentage of ones are {}'.format(per_ones))

BalancingRatio= numNegatives/dataset_size
print('BalancingRatio = {}'.format(BalancingRatio))

dataset=dataset.withColumn("classWeights", when(dataset.default_payment_next_month == 1,BalancingRatio).otherwise(1-BalancingRatio))
dataset.select("classWeights").show(5)

display(dataset.show(3))

cols = dataset.columns


categoricalColumns = ["SEX", "EDUCATION", "MARRIAGE"]
stages = [] # stages in our Pipeline
for categoricalCol in categoricalColumns:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
    # Use OneHotEncoder to convert categorical variables into binary SparseVectors
    # encoder = OneHotEncoderEstimator(inputCol=categoricalCol + "Index", outputCol=categoricalCol + "classVec")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]
    
# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="default_payment_next_month", outputCol="label")
stages += [label_stringIdx]

# Transform all features into a vector using VectorAssembler
numericCols = ["LIMIT_BAL", "AGE", "PAY_0", "PAY_2", "PAY_3", "PAY_3","PAY_4","PAY_5","PAY_6",'BILL_AMT1',
 'BILL_AMT2',
 'BILL_AMT3',
 'BILL_AMT4',
 'BILL_AMT5',
 'BILL_AMT6',
 'PAY_AMT1',
 'PAY_AMT2',
 'PAY_AMT3',
 'PAY_AMT4',
 'PAY_AMT5',
 'PAY_AMT6']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]




#from pyspark.ml.classification import LogisticRegression

lrModel = LogisticRegression(maxIter=10, regParam=0.01, weightCol="classWeights")
stages.append(lrModel)
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(dataset)
#preppedDataDF = pipelineModel.transform(dataset)





#
#display(lrModel, preppedDataDF, "ROC")
#display(lrModel, preppedDataDF)


#preppedDataDF.show(3)

#selectedcols = ["label", "features"] + cols
#dataset = preppedDataDF.select(selectedcols)
#display(dataset)


path = 'tmp/spark-logistic-regression-model6'
pipelineModel.save(path)


sameModel = PipelineModel.load(path)


test = spark.read.option('header', True).option('inferSchema', True).csv("./test.csv")
test = test.withColumnRenamed("default.payment.next.month", "default_payment_next_month")
test.show(2)

prediction = sameModel.transform(test)
prediction.show(3)
prediction.printSchema
selected = prediction.select("ID","default_payment_next_month", "rawPrediction", "probability")
selected.show(1)
selected.printSchema
#for row in selected.collect():
#    rid, actual, prob, prediction = row
#    print((rid, actual, prob, prediction))



from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F
import pyspark.sql.types as T

prob_extract = F.udf(lambda x : float(x[1]), T.FloatType())
#print(prediction.withColumn("prob1",prob_extract("probability")).select("prob1","prediction").show())

evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', metricName = "areaUnderROC", labelCol='default_payment_next_month')
print('Evaluator : ' + str(evaluator.evaluate(prediction))) # 0.7294563666075892

prediction.groupBy('default_payment_next_month','prediction').count().show()
