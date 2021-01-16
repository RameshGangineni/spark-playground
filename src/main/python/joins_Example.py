from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('Joins_Demo'). \
        config("spark.jars", "/opt/spark/2.4.3/jars/mysql-connector-java-5.1.46.jar"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext
    # --------------------------------------------------------------------------------------------
    # Create Dataframes
    emp_data = [
                (1, "Smith", -1, "2018", 10, "M", 3000),
                (2, "Rose", 1, "2010", 20, "M", 4000),
                (3, "Williams", 1, "2010", 10, "M", 1000),
                (4, "Jones", 2, "2005", 10, "F", 2000),
                (5, "Brown", 2, "2010", 40, "", -1),
                (6, "Brown", 2, "2010", 50, "", -1)
               ]
    emp_columns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

    emp_df = spark.createDataFrame(data=emp_data, schema=emp_columns)
    emp_df.show()
    dept_data = [
                    ("Finance", 10),
                    ("Marketing", 20),
                    ("Sales", 30),
                    ("IT", 40)
                ]
    dept_columns = ["dept_name", "dept_id"]
    dept_df = spark.createDataFrame(data=dept_data, schema=dept_columns)
    dept_df.show()
    dept_df.coalesce
    # INNER JOIN
    inner_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="inner").show()
    inner_res_with_select = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="inner").\
        select(['emp_id', 'name', 'superior_emp_id', 'year_joined', 'gender', 'salary', 'dept_name']).show()

    # left == leftouter == left_outer
    left_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="left").show()

    # right == rightouter == right_outer
    right_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="right").show()

    # full == fullouter == full_outer == outer
    outer_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="full").show()

    # semi == leftsemi == left_semi
    left_semi_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="leftsemi").show()

    # anti == leftanti == left_anti
    left_anti_res = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, how="left_anti").show()

    # self
    self_res = emp_df.alias("emp1").join(emp_df.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner").\
        select(col("emp1.emp_id"), col("emp1.name"), col("emp2.emp_id").alias("manager_id"), col("emp2.name").alias("manager_name")).show()

    # Ref: https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/#
    # https://stackoverflow.com/questions/33745964/how-to-join-on-multiple-columns-in-pyspark
