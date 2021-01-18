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

    student_rdd = sc.textFile(
        'file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv', 3)
    student_rdd_without_header = student_rdd.filter(lambda row: 'Marks' not in row)
    pair_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[0], int(x[2])))

    res_rdd = pair_rdd.reduceByKey(lambda accum, n: accum+n)
    print(res_rdd.collect())
    res_rdd1 = res_rdd.map(lambda x: (x[0], x[1]/4))
    print(res_rdd1.collect())
