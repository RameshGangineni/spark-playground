from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.rdd import portable_hash


def print_partitions(df):
    numPartitions = df.rdd.getNumPartitions()
    print("Total partitions: {}".format(numPartitions))
    print("Partitioner: {}".format(df.rdd.partitioner))
    df.explain()
    parts = df.rdd.glom().collect()
    i = 0
    j = 0
    for p in parts:
        print("Partition {}:".format(i))
        for r in p:
            print("Row {}:{}".format(j, r))
            j = j+1
        i = i+1


def country_partitioning(k):
    return countries.index(k)


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('hash_partition'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    # Create RDD from text file
    # rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv')
    # df = spark.read.csv('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv', header=True)
    # print_partitions(df)

    countries = ("CN", "AU", "US")
    data = []
    for i in range(1, 13):
        data.append({"ID": i, "Country": countries[i % 3], "Amount": 10 + i})

    df = spark.createDataFrame(data)
    df.show()
    print_partitions(df)

    # After Repartition
    numPartitions = 3
    df = df.repartition(numPartitions, "Country")
    print_partitions(df)

    # We can see uneven distribution of data

    udf_portable_hash = udf(lambda str: portable_hash(str))
    df = df.withColumn("Hash#", udf_portable_hash(df.Country))
    df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
    df.show()

    # If we increase partition number to 5

    numPartitions = 5
    df = df.repartition(numPartitions, "Country")
    print_partitions(df)
    udf_portable_hash = udf(lambda str: portable_hash(str))
    df = df.withColumn("Hash#", udf_portable_hash(df.Country))
    df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
    df.show()

    # Still data is not evenly distributed

    udf_country_hash = udf(lambda str: country_partitioning(str))

    df = df.rdd \
        .map(lambda el: (el["Country"], el)) \
        .partitionBy(numPartitions, country_partitioning) \
        .toDF()
    print_partitions(df)

    df = df.withColumn("Hash#", udf_country_hash(df[0]))
    df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
    df.show()
    
    # Ref: https://kontext.tech/column/spark/299/data-partitioning-functions-in-spark-pyspark-explained
