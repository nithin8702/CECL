#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 16:55:40 2019

@author: nithin
"""

print('DataSource called')
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from operator import add
import os, time, subprocess
import pkg_resources
import configparser
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
        config = configparser.ConfigParser()
        print('---------------------------------------------------Start-----------------------------------------------------------------------------------------------------')
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        cwd = os.getcwd()
        arr = os.listdir(cwd)
        print('-------------------------------------------------------working directories-----------------------------------------------------------------------------------')
        print(cwd)
        print(arr)
        print('----------------------------------------------------------cmd starts-----------------------------------------------------------------------------------------')
#        cmd = 'hdfs dfs -ls /home/hadoop'.split() # cmd must be an array of arguments
#        files = subprocess.check_output(cmd).strip().split('\n')
#        for path in files:
#          print (path)
        print('-------------------------------applicationId-------------------------------------------------------------------------------------------------------------------')
        appId = sc.applicationId
        print(appId)
        ip = 'ip-172-31-18-164.ec2.internal'
        appPath = 'hdfs://' + ip + ':8020/user/hadoop/.sparkStaging/' + appId + '/config.json'
        iniPath = 'hdfs://' + ip + ':8020/user/hadoop/.sparkStaging/' + appId + '/config.ini'
        print(appPath)
        print(iniPath)
        tmp1 = 'file:///user/hadoop/.sparkStaging/' + appId + '/config.ini'
        print(tmp1)
#        print('-------------------------------------------------------------config reader starts-------------------------------------------------------------------------')
#        strng = open(appPath, 'r').read()
#        print(strng)
        print('----------------------------------open ends---------------------------------------------------------------------------------')
        print(SparkFiles.getRootDirectory())
        print(os.listdir(SparkFiles.getRootDirectory()))
#        cmd = 'hdfs dfs -ls ' + SparkFiles.getRootDirectory() +''.split()
#        files = subprocess.check_output('hdfs dfs -ls ' + SparkFiles.getRootDirectory()).strip().split('\n')
#        for path in files:
#          print (path)
#        stg_path = str(fs.defaultFS) + "/user/" + str(os.environ['USER']) + "/.sparkStaging/" + str(sc.applicationId) + "/" lines = sc.textFile(os.path.join(stg_path,'readme.txt'))
#        print(lines.collect())
        print('--------------------------------------------------getRootDirectory----------------------------------------------------------------------')
        
        
#        configFile = pkg_resources.resource_filename(pkg_resources.Requirement.parse("myapp"), "config.ini")
#        config = ConfigParser.ConfigParser()
#        config.read(configFile)

#        sc.textFile("file:///" + SparkFiles.get("config.ini")).collect().foreach(print)
        print('------------------file--------------------------------------------------------------------------------------------------------------------------')
        conString = ''
        with open(SparkFiles.get('config.ini')) as test_file:
            conString = test_file.read()
            print(conString)
        print('----------------------------------------------------------------config reader ends---------------------------------------------------------------------')
        config.read_string(conString)
        val = config['SPARK']['val']
        print(val)
        print('----------------------------------------------------------val-----------------------------------------------------------------------------------------------')
        #hdfs://ip-172-31-19-25.ec2.internal:8020
#        print('---------------------------------------------Hadoop Files------------------------------------------------------------------------------------------------------')
##        print(SparkFiles.getRootDirectory())
        
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