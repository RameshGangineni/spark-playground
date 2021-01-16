from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('pandas_udf_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
