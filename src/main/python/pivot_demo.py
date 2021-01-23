from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('pivot_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    data = [
            ("Banana", 1000, "USA"),
            ("Carrots", 1500, "USA"),
            ("Beans", 1600, "USA"),
            ("Orange", 2000, "USA"),
            ("Orange", 2000, "USA"),
            ("Banana", 400, "China"),
            ("Carrots", 1200, "China"),
            ("Beans", 1500, "China"),
            ("Orange", 4000, "China"),
            ("Banana", 2000, "Canada"),
            ("Carrots", 2000, "Canada"),
            ("Beans", 2000, "Mexico")
            ]
    columns = ["Product", "Amount", "Country"]
    df = spark.createDataFrame(data=data, schema=columns)
    df1 = df.groupBy("Product").pivot("Country").sum("Amount")
    # df1.show()

    # If we want pivot only on select columns, exclude Mexico here
    countries = ["USA", "China", "Canada"]
    df1 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    # df1.show()

    # Performance of Pivot improved in Spark 2 by splitting the operation into chunks
    pivotDF = df.groupBy("Product", "Country") \
        .sum("Amount") \
        .groupBy("Product") \
        .pivot("Country") \
        .sum("sum(Amount)")
    # pivotDF.show()

    # ------------------------------------------------------------------------------------------------
    data = [(101, 'A', 1000.01),
            (101, 'B', 2000.00),
            (101, 'C', 5000.00),
            (102, 'A', 2000.01),
            (102, 'B', 4000.1),
            (103, 'A', 2000.01),
            (103, 'B', 4000.1),
            (101, 'A', 3000.01)]
    cus_schema = StructType([StructField("Employee", StringType(), True),
                             StructField("Product", StringType(), True),
                             StructField("Amount", FloatType(), True)]
                            )

    df = spark.createDataFrame(data=data, schema=cus_schema)
    columns = ['Employee', 'Product', 'Amount']
    df = sc.parallelize(data).toDF(columns)
    df1 = df.groupBy('Employee').pivot('Product').sum('Amount')
    df1.show()

    df = spark.sql('''
    SELECT * FROM VALUES
(101,'A',1000.01),
(101,'B',2000),
(101,'C',5000),
(102,'A',2000.01),
(102,'B',4000.1),
(103,'A',2000.01),
(103,'B',4000.1),
(101,'A',3000.01)
AS Sales(Employee,Product,Amount)
PIVOT (
SUM(Amount) AS amt, COUNT(Amount) AS cnt
FOR Product IN ( 'A' AS a, 'B' as b, 'C' AS c)
)
    ''')
    df.show(5)
    df = spark.sql('''
    SELECT * FROM VALUES
(101,'A',1,1000.01),
(101,'B',0,2000),
(102,'A',0,2000.01),
(102,'B',1,4000.1),
(103,'A',0,2000.01),
(103,'B',1,4000.1),
(101,'A',1,3000.01)
AS Sales(Employee,Product,Online,Amount)
PIVOT (
SUM(Amount) AS amt
FOR (Product,Online) IN ( ('A',1) AS a_online, ('B',1) as b_online, ('A',0) AS a_other, ('B',0) as b_other)
)
    ''')
    # df.show(5)
