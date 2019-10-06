#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 16:55:40 2019

@author: nithin
"""

print('DataSource called')
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
import os, time, subprocess
#import configparser
from pyspark import SparkFiles
#import json
ts = int(time.time())
#
#with open('config.json') as f:
#  data = json.load(f)
#data['SPARK']['Val']
  
class DataSource:
    def __init__(self):
        """Create a spark context and session"""
        print('__init__ called')
#        config = configparser.ConfigParser()
        print('---------------------------------------------------Start-----------------------------------------------------------------------------------------------------')
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        arr = os.listdir(os.getcwd())
        print(arr)
        print('-------------------------------applicationId-------------------------------------------------------------------------------------------------------------------')
        appId = sc.applicationId
        print(appId)
        appPath = 'hdfs://ip-172-31-29-44.ec2.internal:8020/user/hadoop/.sparkStaging/' + appId + '/config.json'
        print(appPath)
#        config.read(appPath)
#        val = config['SPARK']['val']
#        print(val)
        #hdfs://ip-172-31-19-25.ec2.internal:8020
#        print('---------------------------------------------Hadoop Files------------------------------------------------------------------------------------------------------')
##        print(SparkFiles.getRootDirectory())
#        cmd = 'hadoop fs -ls -R /'
#        files = subprocess.check_output(cmd, shell=True).strip().split('\n')
#        for path in files:
#          print(path)
        print('------------------------------------------------------------Context------------------------------------------------------------------------------------------------')
#        textFile = sc.textFile(appPath)
        conf = spark.read.option("multiline",True).json(appPath)
        print('------------------------------------------------------------------------Spark Read---------------------------------------------------------------------------')
        print(conf.printSchema())        
        print(conf.select('SPARK1').first()[0])
        print(sc.sparkUser())
        print('---------------------------------------------------------completed---------------------------------------------------------------------------------------')
        data = sc.parallelize(list(conf['SPARK']))
        data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).coalesce(1).saveAsTextFile('s3://nithin-emr/' + str(ts) + '/result')
        sc.stop()