from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('unpivot_demo'). \
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

    pivotDF = df.groupBy("Product", "Country") \
        .sum("Amount") \
        .groupBy("Product") \
        .pivot("Country") \
        .sum("sum(Amount)")
    pivotDF.show()

    unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
    unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
        .where("Total is not null")
    unPivotDF.show()
