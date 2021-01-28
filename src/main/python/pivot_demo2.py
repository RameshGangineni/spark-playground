from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('pivot_demo2'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    data = [('foo','one','small',1),
            ('foo','one','large',2),
            ('foo','one','large',2),
            ('foo','two','small',3),
            ('foo','two','small',3),
            ('bar','one','large',4),
            ('bar','one','small',5),
            ('bar','two','small',6),
            ('bar','two','large',7)]
    columns = ['A', 'B', 'C', 'D']
    df = spark.createDataFrame(data=data, schema=columns)
    '''
    Expected Output
    A	B	large	small
    foo	two	null	6
    bar	two	7	6
    foo	one	4	1
    bar	one	4	5
    '''
    df1 = df.groupBy('A', 'B').pivot('C').sum('D')
    df1.show(5)

    # If we want only one column data
    df1 = df.groupBy('A', 'B').pivot('C', ['large']).sum('D')
    df1.show(5)
