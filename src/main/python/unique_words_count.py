from pyspark.sql import SparkSession


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('unique_words_count'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/simple_text.txt')

    lines_rdd = rdd.flatMap(lambda line: line.split(" "))

    words_rdd = lines_rdd.map(lambda word: (word, 1))

    word_count_rdd = words_rdd.reduceByKey(lambda a, b: a+b)
    word_count_rdd1 = word_count_rdd.filter(lambda row: row[1] == 1)
    print(word_count_rdd1.count())
