from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('opposite_inner_join'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    snapshot_df = spark.read.csv(
        'file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/transaction_data.csv',
        header=True, inferSchema=True)
    lookup_df = spark.read.csv(
        'file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/lookup.csv', header=True,
        inferSchema=True)
    new_df = snapshot_df.withColumn("year", snapshot_df['transaction_date'][7:9])
    cols = snapshot_df.columns

    inner_join_df = new_df.join(lookup_df, new_df['year'] == lookup_df['valid_year'], 'inner').select(cols)
    cols1 = ['df1.transaction_id', 'df1.user_name', 'df1.user_type', 'df1.transaction_date']
    opposite_inner_join_df = new_df.alias('df1').join(lookup_df.alias('df2'), new_df['year'] == lookup_df['valid_year'], 'left').where('df2.valid_year is null').select(cols1)

    inner_join_df.show(5)
    opposite_inner_join_df.show(5)
