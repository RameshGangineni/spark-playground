from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('valid_invalid_records'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    '''
    Requirement: Move all the transaction for 2019 into valid and 2018 into involved
    '''

    '''
    snapshot_data = [(1, 'User1', 'New', '20-04-2018'),
                     (2, 'User2', 'Privileged', '21-04-2019'),
                     (3, 'User3', 'Privileged', '20-04-2018'),
                     (4, 'User4', 'New', '21-04-2019'),
                     (5, 'User5', 'New', '20-05-2018'),
                     (6, 'User6', 'New', '26-07-2018'),
                     (7, 'User7', 'Privileged', '21-04-2019'),
                     (8, 'User8', 'Privileged', '07-11-2018'),
                     (9, 'User9', 'New', '17-05-2019'),
                     (10, 'User10', 'New', '26-10-2019')]
    look_up_data = [['2019']]
    snap_shot_schema = StructType([
                                    StructField('transaction_id', IntegerType(), False),
                                    StructField('user_name', StringType(), False),
                                    StructField('user_type', StringType(), False),
                                    StructField('transaction_date', StringType(), False)
                                 ])
    look_up_schema = StructType([
                                    StructField('valid_year', StringType(), False)
                               ])
    snapshot_df = spark.createDataFrame(data=snapshot_data, schema=snap_shot_schema)
    look_up_df = spark.createDataFrame(data=look_up_data, schema=look_up_schema)
    snapshot_df.show(5)
    look_up_df.show(5)
    '''

    snapshot_df = spark.read.csv('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/transaction_data.csv', header=True, inferSchema=True)
    lookup_df = spark.read.csv('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/lookup.csv', header=True, inferSchema=True)
    new_df = snapshot_df.withColumn("year", snapshot_df['transaction_date'][7:9])

    cols = snapshot_df.columns
    valid_records_df = new_df.join(lookup_df, new_df['year'] == lookup_df['valid_year'], 'inner').select(cols)
    valid_records_df.show()

    cols1 =['df1.transaction_id', 'df1.user_name', 'df1.user_type', 'df1.transaction_date']

    # invalid_records_df = snapshot_df.alias('df1').join(valid_records_df.alias('df2'), 'transaction_id', 'left').where('df2.transaction_date is null').select(cols1)
    # invalid_records_df.show()
    invalid_records_df = new_df.alias('df1').join(lookup_df.alias('df2'), new_df['year'] == lookup_df['valid_year'], 'left').where('df2.valid_year is null').select(cols1)
    invalid_records_df.show(5)
