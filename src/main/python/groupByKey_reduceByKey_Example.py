from pyspark.sql import SparkSession


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
    rdd = sc.parallelize(["Naresh", "Suresh", "Santosh", "Naren", "Aravind", "Ashok", "Krishna"], 2)
    group_rdd = rdd.map(lambda word: (word[0], word))
    rdd1 = group_rdd.groupByKey()
    for row in rdd1.collect():
        print(row[0], [i for i in row[1]])

    rdd1 = group_rdd.reduceByKey(lambda accum, n: [accum, n])
    for row in rdd1.collect():
        print([i for i in row])
    # Ref: https://backtobazics.com/big-data/spark/apache-spark-groupby-example/
