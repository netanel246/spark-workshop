### Lab 3: Analyzing Startup Companies

In this lab, you will analyze a real-world dataset -- information about startup companies. The source of this dataset is [jSONAR](http://jsonstudio.com/resources/).

___

#### Task 1: Inspecting the Data

This time, the data is provided as a JSON document, one entry per line. Take a look at the first entry by using the following command(%fs allows us to inspect the DBFS of databricks):

```
%fs head /FileStore/tables/spark-workshop/companies.json
```

As you can see, the schema is fairly complicated -- it has a bunch of fields, nested objects, arrays, and so on. It describes the company's products, key people, acquisition data, and more. We are going to use Spark SQL to infer the schema of this JSON document, and then issue queries using a natural SQL syntax.

___

#### Task 2: Parsing the Data

In Databricks notebooks, you have access to a pre-initialized `SparkSession` object named `spark`.

Create a `DataFrame` from the JSON file so that its schema is automatically inferred, and print out the resulting schema.

Pay attention, to use the **Fluent query API** and not SQL.

**Solution**:

```python
companies = spark.read.json("file:///home/ubuntu/data/companies.json")
companies.printSchema()
```

___

#### Task 3: Querying the Data

First, let's talk about the money; figure out what the average acquisition price was.

**Solution**:

```python
from pyspark.sql.functions import avg,col
companies.select(avg("acquisition.price_amount")).show()
```

Not too shabby. Let's get some additional detail -- print the average acquisition price grouped by number of years the company was active.

**Solution**:

```python
companies.select(col("acquisition.price_amount"),(col("acquisition.acquired_year") - col("founded_year")).alias("years_active")) \
    .where("price_amount is not null") \
    .groupBy("years_active") \
    .avg("price_amount") \
    .withColumnRenamed("avg(price_amount)", "acq_price") \
    .sort("acq_price", ascending = False) \
    .collect()
```

Finally, let's try to figure out the relationship between the company's total funding and acquisition price. In order to do that, you'll need a UDF (user-defined function) that, given a company, returns the sum of all its funding rounds. First, build that function and register it with the name "total_funding".

**Solution**:

```python
from pyspark.sql.functions import udf
@udf("int")
def total_funding(investments):
  return sum([inv.funding_round.raised_amount or 0 for inv in investments])
```

Test your function by retrieving the total funding for a few companies, such as Facebook, PayPal, and Alibaba. Now, find the average ratio between the acquisition price and the total funding (which, in a simplistic way, represents return on investment).

**Solution**:

```python
companies.select(col("acquisition.price_amount") / total_funding("investments")) \
         .withColumnRenamed("(acquisition.price_amount / total_funding(investments))", "roi") \
         .where((col("acquisition.price_amount").isNotNull()) & (total_funding("investments") != 0)) \
         .groupBy() \
         .avg("roi") \
         .show()
```

___

#### Discussion

See discussion for the [next lab](lab4-propprices.md).
