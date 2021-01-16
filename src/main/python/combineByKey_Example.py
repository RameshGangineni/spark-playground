from pyspark.sql import SparkSession
from common.pretty_print import *

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('RDD_Transformations_Actions'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

