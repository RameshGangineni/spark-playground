from pyspark.sql import SparkSession


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('word_count'). \
        config('spark.driver.cores', '2').\
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    # Create RDD from text file
    rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/simple_text.txt')

    # Split lines into words
    lines_rdd = rdd.flatMap(lambda line: line.split(" "))

    # Create Pair RDD by giving 1 to each word
    # transformed_rdd = transformed_rdd.map(lambda x: x.strip('[').strip(']'))
    words_rdd = lines_rdd.map(lambda word: (word, 1))

    # GroupByWord
    word_count_rdd = words_rdd.reduceByKey(lambda a, b: a+b)

    # To View the data
    print(word_count_rdd.take(20))

    # Write data to out path
    word_count_rdd.saveAsTextFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/out/')
