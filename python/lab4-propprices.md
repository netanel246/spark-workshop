### Lab 4: Analyzing UK Property Prices

In this lab, you will work with another real-world dataset that contains residential property sales across the UK, as reported to the Land Registry. You can download this dataset and many others from [data.gov.uk](https://data.gov.uk/dataset/land-registry-monthly-price-paid-data).

___

#### Task 1: Inspecting the Data

As always, we begin by inspecting the data. Run the following command to take a look at some of the entries:

```
%fs head /FileStore/tables/prop-prices.csv
```

Note that this time, the CSV file does not have headers. To determine which fields are available, consult the [guidance page](https://www.gov.uk/guidance/about-the-price-paid-data).

___

#### Task 2: Importing the Data

Next, load the `prop-prices.csv` file as an `DataFrame`, and register it as a temporary table so that you can run SQL queries:

```python
columns = ['id', 'price', 'date', 'zip', 'type', 'new', 'duration', 'PAON',
           'SAON', 'street', 'locality', 'town', 'district', 'county', 'ppd',
           'status']

df = spark.read.option("inferSchema", "true").option("header", "false").csv("file:///home/ubuntu/data/prop-prices.csv")
for i, col in enumerate(df.columns):                                        
     df = df.withColumnRenamed(col, columns[i])
df.registerTempTable("properties")
df.persist()
```

___

#### Task 3: Analyzing Property Price Trends

First, let's do some basic analysis on the data. Find how many records we have per year, and print them out sorted by year.

**Solution**:

```python
display(spark.sql("""select   year(date), count(*)
                  from     properties
                  group by year(date)
                  order by year(date)"""))
```

All right, so everyone knows that properties in London are expensive. Find the average property price by county, and print the top 10 most expensive counties.

**Solution**:

```python
spark.sql("""select   county, avg(price)
                  from     properties
                  group by county
                  order by avg(price) desc
                  limit    10""").collect()
```

Is there any trend for property sales during the year? Find the average property price in Greater London month over month in 2015 and 2016, and print it out by month.

**Solution**:

```python
display(spark.sql("""select   county, avg(price)
                  from     properties
                  group by county
                  order by avg(price) desc
                  limit    10"""))
```
Let's plot the property price changes, month-over-month, across the entire dataset.

> You can do it by clicking the Bar Chart -> Plot Options and configure the plot Bar chart, after you display the results 

**Solution**:

```python
display(spark.sql("""select   year(date), month(date), avg(price)
                     from     properties
                     group by year(date), month(date)
                     order by year(date), month(date)"""))
```
Bar Chart -> Plot Options -> Keys: year, Series groupings: month, Values: avg(price)
___

#### Discussion

Now that you have experience in working with SQL, what are the advantages and disadvantages of using it compared to the Fluent query API?
