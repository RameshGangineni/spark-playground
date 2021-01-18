from pyspark.sql import SparkSession
from pyspark.sql import Row
if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('rows_to_df_demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    # Accessing Columns in Row
    row1 = Row(ID=1, Name='ABC', City='Hyd')
    row2 = Row(ID=2, Name='DEF', City='Chn')
    row3 = Row(ID=3, Name='GHI', City='Hyd')
    print(row1.Name)

    # Create Schema
    Employee = Row("ID", "Name", "City")
    row1 = Employee(1, "ABC", "Hyd")
    row2 = Employee(2, "DEF","Chn")
    row3 = Employee(3, "GHI", "Hyd")
    print(row1.Name)

    # Create Dataframe from Row class
    data = [Employee(1, "ABC", "Hyd"), Employee(2, "DEF","Chn"), Employee(3, "GHI", "Hyd")]

    df = spark.createDataFrame(data=data)
    df.show()
