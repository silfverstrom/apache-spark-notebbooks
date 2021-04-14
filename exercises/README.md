# PySpark Exercises

This is a collection of PySpark exercises.
The solution to all of these problems can be found in the solutions folder.


# To start solving problems with pyspark, we first need to import it

```
%pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
```


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

```
data = list_of_100_numbers()

rdd = sc.parallelize(data) # Convert list to rdd
rdd = rdd.map(lambda_function_that_tripples_number)
result = rdd.sum() # Sum all our values
assert(result == 15150)
```


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

```
rdd = sc.textFile(path)

rdd = rdd.flatMap(function_that_splits_csv)
rddd.collect()

Output should look something like:
['1', '200', '2', '500', '3', '-99', '4', '99', '5', '600']
```



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

```
rdd = rdd.map(parseRow).filter(filterLessThanZero).map(getId)
less_than_zero = rdd.collect()

Output should look something like:
[3]
```


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

```
countries  = {"sv": "sweden", "us": "usa"}
data = ["sv","us"]
broadcast_countries = spark.sparkContext.broadcast(countries)

parallelize, map and collect

Output should look something like:
['sweden', 'usa']
```


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

```

accum=sc.accumulator(0)
read_file()
custom_summary()
assert(accum.value == 1300)

Output should be
1300

```

# 2.1 - Fetch JSON-file from s3 as DataFrame and show its structure


Fetch a json-file from s3 and inspect the data.


Spark functions you will need to use

*  spark.read.option("multiline",True).json(path)

In pseudo code:
```

dataframe = read_json_from_s3_to_df(path)
dataframe.show
```

# 2.2 -  Group all eco codes from the dataframe, sort them and show the highest

 Group all eco codes from the dataframe, sort them and show the highest


Spark functions you will need to use

* df.groupBy - transformation that groups data
* count() - counts number of elements
* df.orderBy - orders results
* df.show - shows results

Setup
```

dataframe = read_json_from_s3_to_df(path)
group_data_frame()
count_group()
orderBy()
```

# 2.3 -  Count how many different variations there are of Alekhines opening

We can find out how many openings there are that starts wit Alekhine by using a
LIKE query in sql.

Spark functions you will need to use

* df.select() - select columns
* df.where() - add sql where query
* df.count() - count occurences

In pseudo code:

```
df = df.select(columns)
df = df.where(query)
ct = df.count()
```

# 2.4 -  Find all openings starting with e4 e5. Define a opening as having the first 10 letters unique.

There are many variations of chess openings. We want to find how many
variations there are for the major groups of openings.
We find the groups by selecting the first 10 letters of an opening, and
grouping the results on this value.

We want a result that looks something like this:
```
+----------+-----+
|Ruy Lopez:|  227|
|King's Gam|  189|
|Italian Ga|  156|
....
```

Spark functions you will need to use:

* df.select
* pyspark.sql.functions.substring().alias(alias_name) - for creating a substring
* df.where - For specifing query
* df.groupBy - for grouping results
* df.count() - for counting elements
* df.orderBy - for ordering resultskk

# 3.0 - More analysis of chess-data.

Lets try finding the solution to a couple of questions from a dataset

For all these exercises, load a dataframe using:

Also, take a look at the reference of built in aggregation functions

* https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions

```
df = spark.read.csv("s3://link-workshops/chessdb.csv", header=True)

# will return a structure looking like
|             Opening|Colour|Num Games|ECO|Last Played|Perf Rating|Avg Player|Player Win %|Draw %|Opponent Win %|               Moves|

```

# 3.1 - What opening has the largest win %?

# 3.2 - What are the top 10 openings that was played in 2018?

# 3.3 - What is the most common first move for black?

# 3.4 - What is the most common move for white for players with an average rating over 2200?

# 3.5 - How many games has been played in total?

# 3.6 - What opening gives the best ratio between Avg Player and Perf Rating.

If performance rating is higher than avg player, Perf Rating - Avg Player is
the ratio above where the player is expected to play. What openings gives us
the best ratio?
