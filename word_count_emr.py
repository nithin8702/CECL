#import os
#print('main called')
#arr = os.listdir(os.getcwd())

#print(arr)
#import configparser
#from pyspark import SparkContext
#from operator import add
#from pyspark import SparkFiles

#print('-----getrootdirectory-------')
#print(SparkFiles.getRootDirectory())
#print('-----getrootdirectory-------')

import data.data_source as da
import subprocess
import os
class Test:    
    print("Starting..------------------------------------------------------------------------------------------------------------------------------.")
    src = da.DataSource()
#    sc = SparkContext()
#    print(sc.applicationId)
    
#    config = configparser.ConfigParser()
#    config.read('config.ini')
#    val = config['SPARK']
#    print(val)
    print("Ending...")
    

if __name__ == '__main__':
    print('--------------------------PySpark Testing Start-------------------------------------------------------------------------------------------')
    Test()
    print('-------------------------PySpark Testing End---------------------------------------------------------------------------------------------')