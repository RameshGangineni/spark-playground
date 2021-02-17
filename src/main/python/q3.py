import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, DateType
import datetime
from datetime import date



def top_three(row):
    lst = row[1]
    l = sorted(lst, reverse=True)
    l1 = (row[0], l[:3])
    return l1


def age_calc(dob):
    print(dob)
    age = datetime.datetime.strptime(dob, '%Y-%m-%d')
    return age


if __name__ == '__main__':
    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession. \
        builder. \
        appName('aggregate_by_key_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    df = spark.read.csv('/home/rameshbabug/Documents/projects/internal/spark-playground/src/main/data_test.csv', header=True, inferSchema=True)
    df.show(5)

    # add year of exam column
    df1 = df.withColumn('year_of_exam', lit('2021'))

    df1.show(5)
    df1.printSchema()

    # calculate age for each student
    end_date_1 = date.today()
    df1 = df1.withColumn('end_date_1', unix_timestamp(lit(end_date_1), 'yyyy-MM-dd').cast("timestamp"))
    df1 = df1.withColumn('age', datediff(col('end_date_1'), col('dob'))/365)
    df1 = df1.withColumn('age', df1['age'].cast(IntegerType()))
    df1.show(5)

    # top 3 marks per student per section
    # pair rdd
    # df
    # sql

    df.createOrReplaceTempView('student')

    # SPARK_SQL
    # ----------------------------------------------------------------------------------------------------------------

    # Student wise top 3 marks
    student_wise_top_3_df = spark.sql(
        'select s.marks, s.name from (select marks, name, row_number() over(partition by name order by marks desc)'
        ' as row_num from student) s where s.row_num < 4')
    student_wise_top_3_df.show()

    # section wise top 3 marks
    section_wise_top_3_df = spark.sql(
        'select s.marks, s.name, s.section from (select marks, name, section, row_number() '
        'over(partition by section order by marks desc) as row_num from student) s where s.row_num < 4')
    section_wise_top_3_df.show()

    # Per Student per section
    per_student_per_section_df = spark.sql(
        'select s.marks, s.name, s.section from (select marks, name, section, row_number() '
        'over(partition by name, section order by marks desc) as row_num from student) s where s.row_num < 4')
    per_student_per_section_df.show()

    # ------------------------------------------------------------------------------------------------------------

    # DF

    # ------------------------------------------------------------------------------------------------------------

    df = df.select(['name', 'marks', 'section'])
    student_wise_top_3_df = df.withColumn("row_num", row_number().over(Window.partitionBy('name').orderBy(col("marks").desc())))
    student_wise_top_3_df.filter(student_wise_top_3_df['row_num'] < 4).drop('row_num').show()

    section_wise_top_3_df = df.withColumn("row_num", row_number().over(Window.partitionBy('section').orderBy(col("marks").desc())))
    section_wise_top_3_df.filter(section_wise_top_3_df['row_num'] < 4).drop('row_num').show()

    per_student_per_section_df = df.withColumn("row_num", row_number().over(Window.partitionBy(['name', 'section']).orderBy(col("marks").desc())))
    per_student_per_section_df.filter(per_student_per_section_df['row_num'] < 4).drop('row_num').show()

    # ------------------------------------------------------------------------------------------------------------

    # RDD
    # ------------------------------------------------------------------------------------------------------------
    student_rdd = sc.textFile('/home/rameshbabug/Documents/projects/internal/spark-playground/src/main/data_test.csv')
    student_rdd_without_header = student_rdd.filter(lambda row: 'marks' not in row)
    spark.udf.register("top_three", top_three, StringType())

    student_wise_top_3_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[0], (int(x[3])))).groupByKey()
    student_wise_top_3_rdd = student_wise_top_3_rdd.map(lambda row: (row[0], [i for i in row[1]])).map(lambda x: top_three(x))

    print(student_wise_top_3_rdd.collect())

    section_wise_top_3_rdd = student_rdd_without_header.map(lambda row: row.split(",")).map(lambda x: (x[4], (int(x[3])))).groupByKey()
    section_wise_top_3_rdd = section_wise_top_3_rdd.map(lambda row: (row[0], [i for i in row[1]])).map(lambda x: top_three(x))

    print(section_wise_top_3_rdd.collect())


'''
spark-submit \
--conf spark.executor.memory=4G \
--conf spark.executor.cores=5 \
--conf spark.executor.instances=3 \
--conf spark.executor.memoryOverhead=2G \
--conf spark.driver.memory=6G \
--conf spark.default.parallelism=60 \
--conf spark.driver.memoryOverhead=2G \
--conf spark.driver.maxResultSize=1G \
--conf spark.sql.shuffle.partitions=50 q3.py \
--spark_master=yarn
'''