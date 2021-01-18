from pyspark.sql import SparkSession
from common.pretty_print import *


def process_partition_sum(iterator):
    sum1 = 0
    for element1 in iterator:
        sum1 = sum1 + element1
    yield sum1


def process_partition_sum_with_index(index, partition):
    sum1 = 0
    for element1 in partition:
        sum1 = sum1 + element1
    yield index, sum1


def filter_out_2_from_partition(list_of_lists):
    itr = []
    for sub_list in list_of_lists:
        itr.append([x for x in sub_list if x != 2])
    return iter(itr)


def seq_op(accumulator, element):
    return accumulator + element


def comb_op(accumulator1, accumulator2):
    return accumulator1 + accumulator2


def sumFun(accum, n):
    return accum + n


if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('RDD_Transformations_Actions'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')

    # map
    ''' Map transformation returns a new RDD by applying a function to each element of this RDD '''
    # Input elements N and output elements N
    print_info("Input RDD Rows Count: {}".format(emp_data_rdd.count()))
    transformed_rdd = emp_data_rdd.map(lambda x: x.split(","))
    print(transformed_rdd.collect())
    print_info("Transformed RDD Rows Count: {}".format(transformed_rdd.count()))

    # ---------------------------------------------------------------------------------------------

    # flatMap
    ''' flatMap is similar to map, because it applies a function to all elements in a RDD.
        But, flatMap flattens the results '''
    # Input elements N and out elements will be M
    print_info("Input RDD Rows Count: {}".format(emp_data_rdd.count()))
    transformed_rdd = emp_data_rdd.flatMap(lambda x: x.split(","))
    print(transformed_rdd.collect())
    print_info("Transformed RDD Rows Count: {}".format(transformed_rdd.count()))

    # ---------------------------------------------------------------------------------------------

    # filter
    ''' Create a new RDD bye returning only the elements that satisfy filter condition '''
    transformed_rdd = emp_data_rdd.filter(lambda x: 'Hyd' in x)
    print(transformed_rdd.collect())

    # ---------------------------------------------------------------------------------------------
    # mapPartitions
    ''' mapPartition should be thought of as a map operation over partitions and not over the elements of the partition
    It's input is the set of current partitions its output will be another set of partitions.
    
    mapPartitions() can be used as an alternative to map() and foreach(). mapPartitions() can be called 
    for each partitions while map() and foreach() is called for each elements in an RDD 
    
    Spark mapPartitions() provides a facility to do heavy initializations (for example Database connection) 
    once for each partition instead of doing it on every DataFrame row. This helps the performance of the job 
    when you dealing with heavy-weighted initialization on larger datasets.'''

    # e.g. 1
    number_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    num_rdd = sc.parallelize(number_list, 3)
    print_info("Input RDD: {}".format(num_rdd.collect()))
    print("Number of partitions: {}".format(num_rdd.getNumPartitions()))
    num_process_rdd = num_rdd.mapPartitions(process_partition_sum)
    print_info("Processed RDD: {}".format(num_process_rdd.collect()))

    # e.g. 2
    number_list = [ [1, 2, 3], [3, 2, 4], [5, 2, 7] ]
    number_list_rdd = sc.parallelize(number_list)
    filtered_lists = number_list_rdd.mapPartitions(filter_out_2_from_partition)
    print("Filtered List: {}".format(filtered_lists.collect()))

    # -----------------------------------------------------------------------------------------------

    # mapPartitionsWithIndex

    # https://stackoverflow.com/questions/33655920/when-to-use-mapparitions-and-mappartitionswithindex

    ''' Similar to mapPartitions, but also provides a function with an int value to indicate 
    the index position of the partition. '''

    number_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    num_rdd = sc.parallelize(number_list, 3)
    print_info("Input RDD: {}".format(num_rdd.collect()))
    print("Number of partitions: {}".format(num_rdd.getNumPartitions()))
    num_process_rdd = num_rdd.mapPartitionsWithIndex(process_partition_sum_with_index)
    print_info("Processed RDD With Index: {}".format(num_process_rdd.collect()))

    # -----------------------------------------------------------------------------------------------
    # sample
    ''' Return a random sample subset RDD of the input RDD '''
    text_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/simple_text.txt')
    print_info("Original RDD Count: {}".format(text_rdd.count()))
    sample_text_rdd = text_rdd.sample(withReplacement=False, fraction=0.5, seed=1)
    print("Sampled RDD Count: {}".format(sample_text_rdd.count()))

    # ------------------------------------------------------------------------------------------------

    # union
    ''' Return the union of two RDDs '''
    num_rdd1 = sc.parallelize([1, 2, 3, 4, 5], 2)
    num_rdd2 = sc.parallelize([6, 7, 8, 9, 1], 2)
    num_rdd3 = num_rdd1.union(num_rdd2)
    print_info("Union RDD: {}".format(num_rdd3.collect()))

    # -----------------------------------------------------------------------------------------------

    # intersection
    ''' Similar to union but return the intersection of two RDDs  '''
    num_rdd1 = sc.parallelize([1, 2, 3, 4, 5], 2)
    num_rdd2 = sc.parallelize([6, 7, 8, 9, 1], 2)
    num_rdd3 = num_rdd1.intersection(num_rdd2)
    print("Intersection RDD: {}".format(num_rdd3.collect()))

    # -----------------------------------------------------------------------------------------------

    # distinct
    ''' Return a new RDD with distinct elements within a source RDD '''
    num_rdd1 = sc.parallelize([1, 2, 3, 4, 2, 3, 2, 5], 2)
    num_rdd2 = num_rdd1.distinct()
    print_info("Distinct RDD: {}".format(num_rdd2.collect()))

    # ----------------------------------------------------------------------------------------------

    # groupBy
    ''' Spark RDD groupBy function returns an RDD of grouped items '''
    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    print("EMP RDD: {}".format(emp_data_rdd.collect()))
    city_rdd = emp_data_rdd.map(lambda x: x.split(",")[3])
    print_info("CITY RDD: {}".format(city_rdd.collect()))
    city_group = city_rdd.groupBy(lambda city: city[0])
    for element in city_group.collect():
        print(element[0], [i for i in element[1]], sep='')

    # ----------------------------------------------------------------------------------------------

    # mapValues
    ''' If we use map() with a Pair RDD, we get access to both Key & value. There are times we might only be interested
     in accessing the value(& not key). In such case, we can use mapValues() instead of map(). '''

    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    lines_rdd = emp_data_rdd.map(lambda x: x.split(","))
    pair_rdd = lines_rdd.map( lambda x: (x[2], x[1]))
    print("Pair RDD: {}".format(pair_rdd.collect()))
    salary_rdd = pair_rdd.mapValues(lambda salary: (salary, 1))
    print_info("Salary RDD: {}".format(salary_rdd.collect()))
    # ---------------------------------------------------------------------------------------------

    # groupByKey
    ''' When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs '''
    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    # Easiest way to remove header from RDD
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    lines_rdd = emp_data_rdd.map(lambda x: x.split(","))
    group_by_city = lines_rdd.map( lambda x: (x[3], int(x[2]))).groupByKey()
    group_by_city_map = group_by_city.map(lambda x: (x[0], sum(x[1])))
    print("GroupByKey: {}".format(group_by_city_map.collect()))

    # ----------------------------------------------------------------------------------------------

    # reduceByKey
    ''' Operates on key, value pairs again, but the func must be of type (V,V) => V '''

    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    lines_rdd = emp_data_rdd.map(lambda x: x.split(","))
    group_salary = lines_rdd.map( lambda x: (x[3], int(x[2]))).reduceByKey(lambda accum, n: accum+n)
    reduce_by_salary_map = group_salary.map(lambda x: (x[0], x[1]))
    print_info("ReduceByKey {}".format(reduce_by_salary_map.collect()))

    reduceBySalary = lines_rdd.map( lambda x: (x[3], int(x[2])))
    reduceBySalary = reduceBySalary.reduceByKey(sumFun)
    print_debug("ReduceByKey: {}".format(reduceBySalary.collect()))
    # ----------------------------------------------------------------------------------------------

    # sortByKey
    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    lines_rdd = emp_data_rdd.map(lambda x: x.split(","))
    pair_rdd = lines_rdd.map(lambda x: (x[2], x[1])).sortByKey()
    print("SortByKey: {}".format(pair_rdd.collect()))
    pair_rdd = lines_rdd.map(lambda x: (x[2], x[1])).sortByKey(False)
    print_info("SortByKey in Reverse: {}".format(pair_rdd.collect()))
    # -----------------------------------------------------------------------------------------------

    # aggregateByKey

    ''' Spark aggregateByKey function aggregates the values of each key, 
    using given combine functions and a neutral “zero value” '''

    emp_data_rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data.csv')
    emp_data_rdd = emp_data_rdd.filter(lambda x: 'NAME' not in x)
    lines_rdd = emp_data_rdd.map(lambda x: x.split(","))
    # pair_rdd = lines_rdd.map(lambda x: (x[3], x[2])).aggregateByKey(0,)

    # e.g.2
    # Defining Sequential Operation and Combiner Operations
    # Sequence operation : Finding Maximum Marks from a single partition
    def seq_op(accumulator, element):
        if (accumulator > element[1]):
            return accumulator
        else:
            return element[1]

    # Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
    def comb_op(accumulator1, accumulator2):
        if (accumulator1 > accumulator2):
            return accumulator1
        else:
            return accumulator2

    student_rdd = sc.parallelize([
        ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
        ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)], 3)
    zero_val =0
    aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2]))).aggregateByKey(zero_val, seq_op, comb_op)
    print("AggregateByKey: {}".format(aggr_rdd.collect()))
    # Ref: https://backtobazics.com/big-data/spark/apache-spark-aggregatebykey-example/
