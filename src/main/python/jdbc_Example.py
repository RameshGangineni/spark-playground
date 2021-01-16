from pyspark.sql import SparkSession


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('JDBC_Demo'). \
        config("spark.jars", "/opt/spark/2.4.3/jars/mysql-connector-java-5.1.46.jar"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext
    # --------------------------------------------------------------------------------------------
    url = "jdbc:mysql://localhost/practice?autoReconnect=true&useSSL=false"
    properties = {
        "user": "root",
        "password": "********",  # Update password here
        "driver": "com.mysql.jdbc.Driver"
    }
    df = spark.read.jdbc(url=url, table="Employee", properties=properties)
    df.printSchema()
    df.show()
