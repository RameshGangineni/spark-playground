from pyspark.sql import SparkSession


def cal_sum(accum, n):
    return accum+n


if __name__ == '__main__':
    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession. \
        builder. \
        appName('reduce_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    x = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
    sum1 = x.reduce(lambda accum, n: accum+n)
    print(sum1)
    mul = x.reduce(lambda accum, n: accum*n)
    print(mul)

    sum2 = x.reduce(cal_sum)
    print(sum2)
