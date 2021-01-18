from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

'''
when s in name then update address to null and update name to name_checked

Input:
s_id,name,addr,deptid,pocket_money
1,jdsfgh,kjdf,21,100
2,dsfjh,sdkjjfb,21,170
3,ssbf,KFDB,21,130
4,KDSFB,DKFJ,22,240
5,SJFHDJH,nfdj,22,79

Output:
[
['1', 'jdsfgh', 'kjdf', '21', '100'], 
['2', 'dsfjh', 'sdkjjfb', '21', '170'], 
['3', 'ssbf-checked', None, '21', '130'], 
['4', 'KDSFB', 'DKFJ', '22', '240'], 
['5', 'SJFHDJH-checked', None, '22', '79']
]

'''


def cus_udf(row):
    print(row[1][0])
    if 's' in row[1][0].lower():
        row[1] = row[1] + "-checked"
        row[2] = None
    return row


spark = SparkSession. \
    builder. \
    appName('rdd'). \
    enableHiveSupport(). \
    getOrCreate()

sc = spark.sparkContext
rdd = sc.textFile('/home/rameshbabug/Documents/projects/internal/spark-playground/src/data/rdd_data.txt')
spark.udf.register("cus_udf", cus_udf, StringType())
rdd1 = rdd.filter(lambda row: 'name' not in row).map(lambda row: row.split(',')).map(lambda x: cus_udf(x))
print(rdd1.collect())
