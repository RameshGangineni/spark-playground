from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, unix_timestamp, to_timestamp, col
if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('date_format_conversion'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    snapshot_df = spark.read.csv(
        'file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/transaction_data.csv',
        header=True, inferSchema=True)
    snapshot_df.printSchema()
    snapshot_df.show(5)

    # e.g 1
    new_df = snapshot_df.withColumn('transaction_date_col', to_date(unix_timestamp(col('transaction_date'), 'dd-MM-yyyy').cast("timestamp")))
    new_df = new_df.drop(new_df['transaction_date']).withColumnRenamed('transaction_date_col', 'transaction_date')
    new_df.printSchema()
    new_df.show(5)

    # e.g. 2
    new_df = snapshot_df.select(to_date(snapshot_df['transaction_date'], 'dd-MM-yyyy').alias('transaction_date_new'))
    new_df.printSchema()
    new_df.show(5)

    # e.g. 3
    new_df = snapshot_df.select(to_timestamp(snapshot_df['transaction_date'], 'dd-MM-yyyy').alias('transaction_date_new'))
    new_df.printSchema()
    new_df.show(5)
