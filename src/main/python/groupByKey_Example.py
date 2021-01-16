from pyspark.sql import SparkSession
from common.pretty_print import *


def partition_wise_elements(index, partition):
    l1 = []
    for element in partition:
        l1.append(element)
    yield index, l1


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
    student_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/student_data.csv', 3)
    student_rdd_without_header = student_rdd.filter(lambda row: 'Marks' not in row)
    partition_wise_element = student_rdd_without_header.mapPartitionsWithIndex(partition_wise_elements)
    print_info("Partition Wise Elements: {}".format(partition_wise_element.collect()))
    pair_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[0], int(x[2])))
    print_info("Pair RDD: {}".format(pair_rdd.collect()))

    # group by student
    student_wise_marks = pair_rdd.groupByKey(2)
    print_debug("Student Wise Marks")
    for row in student_wise_marks.collect():
        print(row[0], [i for i in row[1]])

    # Ref: https://backtobazics.com/big-data/spark/apache-spark-groupbykey-example/
