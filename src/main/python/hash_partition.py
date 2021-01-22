from pyspark.sql import SparkSession


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
    rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv')

    # Check number of partitions
    print(rdd.getNumPartitions())

    lines_rdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

    print(lines_rdd.getNumPartitions())
    # Number of input partitions and output partitions are same, AS we have two partitions, all the elements  with mod 0
    # will go to partition 0 and all the elements hash code 1 will go partition 1

    # Below two words have two different hashes. When we try to repartition then into 2 partitions one will go
    # partition 0 and other will go to partition 1
    print(hash("Hello") % 2)
    print(hash("Hi") % 2)

    # Partition of two words with same characters won't have same hash value
    print(hash("cat"))
    print(hash("act"))
    lines_rdd.saveAsTextFile('/home/rameshbabug/Documents/projects/internal/spark-playground/src/data/out1/')
