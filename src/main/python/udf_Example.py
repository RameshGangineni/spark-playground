from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr


def uppercase(s):
    return s.upper()


@udf(returnType=StringType())
def uppercaseannotationudf(s):
    return s.upper()


if __name__ == '__main__':
    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession. \
        builder. \
        appName('UDF_Demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    columns = ['ID', 'Name', 'Location']
    data = \
        [
            (1, 'suresh kumar', 'hyderabad'),
            (2, 'naresh', 'bangalore'),
            (3, 'arun', ''),
            (4, 'rahul', 'delhi'),
            (5, 'anil', 'hyderabad'),
            (6, 'ajay', 'bangalore')
        ]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)
    # Convert python function to udf
    convert_case_udf = udf(lambda row: convertCase(row), StringType())

    # Using UDF in pyspark dataframe select
    df.select(col('ID'), convert_case_udf(col('Name')).alias('Name'), col('Location')).show()

    # Using UDF with withColumn
    upper_case_udf = udf(lambda row: uppercase(row))
    df.withColumn("Curated Location", upper_case_udf(col("Location"))).show()

    # Register pyspark udf
    spark.udf.register("convertUDF", convertCase, StringType())
    df.createOrReplaceTempView("Employee")
    spark.sql("SELECT ID, convertUDF(Name) as Name, Location from Employee").show()

    # Create UDF Using annotation
    df.withColumn("Cureated Name", uppercaseannotationudf(col("Name"))).show(truncate=False)

    # Ref: https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/
