from pyspark.sql import SparkSession

'''
max number of space count in a line
'''


def get_count(row):
    counter = 0
    for word in row:
        counter += 1
        print(word)
    return counter


def get_count_new(row):
    n = row.count(' ')
    return n


if __name__ == '__main__':
    spark = SparkSession.\
        builder.\
        master('local[*]'). \
        appName('test_case_q1'). \
        getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile('/home/rameshbabug/Documents/projects/internal/spark-playground/src/data/simple_text.txt')
    # print(rdd.collect())
    rdd1 = rdd.filter(lambda line: 'not' not in line).map(get_count_new)
    # print(rdd1.collect())
    df = rdd1.map(lambda x: [x, ]).toDF(['col1'])
    # df.show()

    df1 = df.agg({"col1": "min"}).collect()[0][0]
    print(df1)

    df1 = df.agg({"col1": "max"})
    df1.show()
