from pyspark.sql import SparkSession


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('groupby_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    rdd = sc.parallelize(["Naresh", "Suresh", "Santosh", "Naren", "Aravind", "Ashok", "Krishna"], 2)
    group_rdd = rdd.groupBy(lambda word: word[0])

    print(group_rdd.collect())
    for row in group_rdd.collect():
        print(row[0], [i for i in row[1]])
    # Ref: https://backtobazics.com/big-data/spark/apache-spark-groupby-example/
