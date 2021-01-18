from pyspark.sql import SparkSession
from common.pretty_print import *


def createCombiner(tpl):
    return tpl[1], 1


def mergeValue(accumulator, element):
    return accumulator[0] + element[1], accumulator[1] + 1


def mergeCombiner(accumulator1, accumulator2):
    return accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1]


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession. \
        builder. \
        appName('combine_by_key_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    student_rdd = sc.textFile(
        'file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv', 3)
    student_rdd_without_header = student_rdd.filter(lambda row: 'Marks' not in row)
    pair_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[0], x[1], int(x[2])))

    comb_rdd = pair_rdd.map(lambda t: (t[0], (t[1], t[2]))).combineByKey(createCombiner, mergeValue, mergeCombiner) \
        .map(lambda t: (t[0], t[1][0] / t[1][1]))

    for tpl in comb_rdd.collect():
        print_info(tpl)
