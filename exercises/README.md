# PySpark Exercises

This is a collection of PySpark exercises.
The solution to all of these problems can be found in the solutions folder.


# To start solving problems with pyspark, we first need to import it

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# 1.1 First Exercise

This is our first test of PySpark. In this simple exercise we are going to create a RDD from a list in python.
We are then going to apply a transformer, map, and tripple the values in the RDD.

We will then use a action, sum, that returns the total sum of all values.
We will then assert that the total sum is 15150.

Spark functions you will need to use,

* sc.parallelize # To convert list to RDD
* rdd.map - to transform data
* sum - to get total sum


In pseudo code:

data = list_of_100_numbers()

rdd = sc.parallelize(data) # Convert list to rdd
rdd = rdd.map(lambda_function_that_tripples_number)
result = rdd.sum() # Sum all our values
assert(result == 15150)


# 1.2 Read a csv and split the columns

In this exercise we will read a csv from s3.
We will then use a transformer, flatMap, that splits the columns for us.

We will then collect all results to see that they are parsed correctly.

The path to the data is:

s3://link-workshops/example_data.csv


Spark functions you will need to use,

* sc.textFile(path) # To get RDD from csv
* rdd.flatMap - to transform data to cols
* rdd.collect - To collect results


In pseudo code:

rdd = sc.textFile(path)

rdd = rdd.flatMap(function_that_splits_csv)
rddd.collect()

Output should look something like:
['1', '200', '2', '500', '3', '-99', '4', '99', '5', '600']



# 1.3 # Read a csv from s3. Find users with less score than zero, and return their ids.


In this exercise we will use our previously loaded csv-file.
Our csv-file has the following structure:
    0: user_id
    1: score
        
We will then find all rows that have a score below zero, and return their id.

We will then collect all results.


Spark functions you will need to use,

* sc.map(path) - To transform data
* rdd.filter - to filter data
* rdd.collect - To collect results


In pseudo code:

rdd = rdd.map(parseRow).filter(filterLessThanZero).map(getId)
less_than_zero = rdd.collect()

Output should look something like:
[3]


# 1.4 Take a list of values, and translate it through a lookup table.

In this exercise we will take a list of values, and translate it through a lookup table.
But to save time, broadcast a read only variable first.

We will then find all rows that have a score below zero, and return their id.

We will then collect all results.


Spark functions you will need to use,

* sc.spark.sparkContext.broadcast(variabble) - To broadcast data between nodes
* spark.sparkContext.parallelize - create rdd from list
* rdd.map - To convert results
* rdd.collect - To collect results


The setup should look like:

countries  = {"sv": "sweden", "us": "usa"}
data = ["sv","us"]
broadcast_countries = spark.sparkContext.broadcast(countries)

parallelize, map and collect

Output should look something like:
['sweden', 'usa']


# 1.5 Create an accumulator and implement a custom sum operation.


Read our previously loaded csv-file.
Then sum the scores of the example_data.csv


Spark functions you will need to use,

* sc.accumulator(0) - to create accumulator
* accum.add - to add values to accumulator
* accum.value - to get accumulator values
* rdd.map - To convert results
* rdd.foreach - To to iterate over results

The setup should look like:

accum=sc.accumulator(0)
read_file()
custom_summary()
assert(accum.value == 1300)

Output should be
1300


