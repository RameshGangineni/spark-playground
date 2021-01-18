from pyspark.sql import SparkSession
from common.pretty_print import *


def seq_op(accumulator, element):
    if accumulator > element[1]:
        return accumulator
    else:
        return element[1]


def comb_op(accumulator1, accumulator2):
    if accumulator1 > accumulator2:
        return accumulator1
    else:
        return accumulator2


def partition_wise_elements(index, partition):
    l = []
    for element in partition:
        l.append(element)
    yield index, l


def seq_op1(accumulator, element):
    if accumulator[1] > element[1]:
        return accumulator
    else:
        return element


def comb_op1(accumulator1, accumulator2):
    if accumulator1[1] > accumulator2[1]:
        return accumulator1
    else:
        return accumulator2


def seq_op2(accumulator, element):
    return accumulator[0] + element[1], accumulator[1] + 1


def comb_op2(accumulator1, accumulator2):
    return accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1]


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('aggregate_by_key_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    student_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv', 3)
    student_rdd_without_header = student_rdd.filter(lambda row: 'Marks' not in row)
    partition_wise_element = student_rdd_without_header.mapPartitionsWithIndex(partition_wise_elements)
    print_info("Partition Wise Elements: {}".format(partition_wise_element.collect()))
    pair_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[0], (x[1], int(x[2]))))
    print_info("Pair RDD: {}".format(pair_rdd.collect()))

    # Max marks student wise
    zero_val = 0
    max_marks_rdd = pair_rdd.aggregateByKey(zero_val, seq_op, comb_op)
    print_error("Max Marks Student Wise: {}".format(max_marks_rdd.collect()))

    # Subject name along with Maximum Marks
    zero_val = ('', 0)
    max_marks_with_subject = pair_rdd.aggregateByKey(zero_val, seq_op1, comb_op1)
    print_debug("Max Marks with Subject Name: {}".format(max_marks_with_subject.collect()))

    # percentage of all students
    zero_val = (0, 0)
    student_percentage = pair_rdd.aggregateByKey(zero_val, seq_op2, comb_op2).map(lambda x: (x[0], x[1][0]/x[1][1]*1.0))
    print_error("Student Wise Percentage: {}".format(student_percentage.collect()))
    # Ref: https://backtobazics.com/big-data/spark/apache-spark-aggregatebykey-example/
    # https://stackoverflow.com/questions/43364432/spark-difference-between-reducebykey-vs-groupbykey-vs-aggregatebykey-vs-combineb
