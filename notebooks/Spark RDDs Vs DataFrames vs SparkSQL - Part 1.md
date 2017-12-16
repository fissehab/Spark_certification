
##  Spark RDDs Vs DataFrames vs SparkSQL - Part 1 : Retrieving, Sorting and Filtering

Spark is a fast and general engine for large-scale data processing. It is a cluster computing framework which is used for scalable and efficient analysis of big data. With Spark, we can use many machines, which divide the tasks among themselves, and perform fault tolerant computations by distributing the data over a cluster.

Among the many capabilities of Spark, which made it famous, is its ability to be used with various programing languages through APIs. We can write Spark operations in Java, Scala, Python or R. Spark runs on Hadoop, Mesos, standalone, or in the cloud. It can access diverse data sources including HDFS, Cassandra, HBase, and S3.

Spark components consist of Core Spark, Spark SQL, MLlib and ML for machine learning and GraphX for graph analytics. To help big data enthusiasts prepare for Apache Spark certifications from companies such as Cloudera, Hortonworks and Databricks, I have started writing tutorials. The first one is [here](http://datascience-enthusiast.com/Python/analyzing_bible_quran_with_spark.html) and the second one is [here](http://datascience-enthusiast.com/Python/SparkDataFrames-ExploringChicagoCrimes.html). For the next couple of weeks, I will write a blog post series on how to perform the same tasks using Spark DataFrames and Spark SQL and this is the first one. I am using pyspark, which is the Spark Python API that exposes the Spark programming model to Python.  The data can be downloaded from my [GitHub repository](https://github.com/fissehab/Spark_certification/tree/master/data/AdventureWorksLT2012). The size of the data is not large, however, the same code works for large volume as well. Therefore, we can practice with this dataset to master the functinalities of Spark.

For this tutorial, we will work with the **SalesLTProduct.txt** data. Let's answer a couple of questions using Spark Resilient Distiributed (RDD) way and DataFrame way.

SparkContext is main entry point for Spark functionality.


```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)
```

** Create RDD from file**


```python
products = sc.textFile("SalesLTProduct.txt")
```

**Retrieve the first row of the data**


```python
products.first()
```




    'ProductID\tName\tProductNumber\tColor\tStandardCost\tListPrice\tSize\tWeight\tProductCategoryID\tProductModelID\tSellStartDate\tSellEndDate\tDiscontinuedDate\tThumbNailPhoto\tThumbnailPhotoFileName\trowguid\tModifiedDate'



We see that the first row is column names and the data is tab (\t) delimited. Let's remove the first row from the RDD and use it as column names.

We can see how many column the data has by spliting the first row as below


```python
print("The data has {} columns".format(len(products.first().split("\t"))))
products.first().split("\t")
```

    The data has 17 columns





    ['ProductID',
     'Name',
     'ProductNumber',
     'Color',
     'StandardCost',
     'ListPrice',
     'Size',
     'Weight',
     'ProductCategoryID',
     'ProductModelID',
     'SellStartDate',
     'SellEndDate',
     'DiscontinuedDate',
     'ThumbNailPhoto',
     'ThumbnailPhotoFileName',
     'rowguid',
     'ModifiedDate']




```python
header = products.first()

content = products.filter(lambda line: line != header)
```

Now, we can see the first row in the data, after removing the column names. 


```python
content.first()
```




    '680\tHL Road Frame - Black, 58\tFR-R92B-58\tBlack\t1059.31\t1431.50\t58\t1016.04\t18\t6\t1998-06-01 00:00:00.000\tNULL\tNULL\t0x47494638396150003100F70000000000800000008000808000000080800080008080808080C0C0C0FF000000FF00FFFF000000FFFF00FF00FFFFFFFFFF000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\tno_image_available_small.gif\t43DD68D6-14A4-461F-9069-55309D90EA7E\t2004-03-11 10:01:36.827'



We have seen above using the header that the data has 17 columns. We can also check from the **content** RDD.


```python
content.map(lambda line: len(line.split("\t"))).distinct().collect()
```




    [17]



Now, let's solve questions using Spark RDDs and Spark DataFrames.

####  1. Transportation costs are increasing and you need to identify the heaviest products. Retrieve the names of the top 15 products by weight.

**RDD Way**

First, we will filter out NULL values because they will create problems to convert the wieght to numeric. Then, we will order our RDD using the weight column in descending order and then we will take the first 15 rows.


```python
(content.filter(lambda line: line.split("\t")[7] != "NULL")
 .map(lambda line: (line.split("\t")[1], float(line.split("\t")[7])))
                     .takeOrdered(15, lambda x : -x[1])
                    )
```




    [('Touring-3000 Blue, 62', 13607.7),
     ('Touring-3000 Yellow, 62', 13607.7),
     ('Touring-3000 Blue, 58', 13562.34),
     ('Touring-3000 Yellow, 58', 13512.45),
     ('Touring-3000 Blue, 54', 13462.55),
     ('Touring-3000 Yellow, 54', 13344.62),
     ('Touring-3000 Yellow, 50', 13213.08),
     ('Touring-3000 Blue, 50', 13213.08),
     ('Touring-3000 Yellow, 44', 13049.78),
     ('Touring-3000 Blue, 44', 13049.78),
     ('Mountain-500 Silver, 52', 13008.96),
     ('Mountain-500 Black, 52', 13008.96),
     ('Mountain-500 Silver, 48', 12891.03),
     ('Mountain-500 Black, 48', 12891.03),
     ('Mountain-500 Silver, 44', 12759.49)]



**DataFrame Way**

Hortonworks Spark Certification is with Spark 1.6 and that is why I am using SQLContext here. Otherwise, for recent Spark versions, SQLContext has been replaced by SparkSession as noted [here](https://spark.apache.org/docs/2.0.0/sql-programming-guide.html#migration-guide)


```python
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)
```


```python
rdd1 = (content.filter(lambda line: line.split("\t")[7] != "NULL")
 .map(lambda line: (line.split("\t")[1], float(line.split("\t")[7])))
)                
```

Now, we can create a DataFrame, order the DataFrame by weight in descending order and take the first 15 records.


```python
df = sqlContext.createDataFrame(rdd1, schema = ["Name", "Weight"])
```


```python
df.orderBy("weight", ascending = False).show(15, truncate = False)
```

    +-----------------------+--------+
    |Name                   |Weight  |
    +-----------------------+--------+
    |Touring-3000 Blue, 62  |13607.7 |
    |Touring-3000 Yellow, 62|13607.7 |
    |Touring-3000 Blue, 58  |13562.34|
    |Touring-3000 Yellow, 58|13512.45|
    |Touring-3000 Blue, 54  |13462.55|
    |Touring-3000 Yellow, 54|13344.62|
    |Touring-3000 Blue, 50  |13213.08|
    |Touring-3000 Yellow, 50|13213.08|
    |Touring-3000 Yellow, 44|13049.78|
    |Touring-3000 Blue, 44  |13049.78|
    |Mountain-500 Black, 52 |13008.96|
    |Mountain-500 Silver, 52|13008.96|
    |Mountain-500 Silver, 48|12891.03|
    |Mountain-500 Black, 48 |12891.03|
    |Mountain-500 Silver, 44|12759.49|
    +-----------------------+--------+
    only showing top 15 rows
    


The **sql** function on a **SQLContext** enables applications to run SQL queries programmatically and returns the result as a DataFrame.

First, we have to register the DataFrame as a SQL temporary view.

** Running SQL Queries Programmatically**


```python
df.createOrReplaceTempView("df_table")
```


```python
sqlContext.sql(" SELECT * FROM df_table  ORDER BY Weight DESC limit 15").show()
```

    +--------------------+--------+
    |                Name|  Weight|
    +--------------------+--------+
    |Touring-3000 Yell...| 13607.7|
    |Touring-3000 Blue...| 13607.7|
    |Touring-3000 Blue...|13562.34|
    |Touring-3000 Yell...|13512.45|
    |Touring-3000 Blue...|13462.55|
    |Touring-3000 Yell...|13344.62|
    |Touring-3000 Yell...|13213.08|
    |Touring-3000 Blue...|13213.08|
    |Touring-3000 Blue...|13049.78|
    |Touring-3000 Yell...|13049.78|
    |Mountain-500 Blac...|13008.96|
    |Mountain-500 Silv...|13008.96|
    |Mountain-500 Blac...|12891.03|
    |Mountain-500 Silv...|12891.03|
    |Mountain-500 Silv...|12759.49|
    +--------------------+--------+
    


** 2.  The heaviest ten products are transported by a specialist carrier, therefore you need to modify the previous query to list the heaviest 15 products not including the heaviest 10.**

First, let's remove the top 10 heaviest ones and take the top 15 records based on the weight column.

**RDD way**


```python
top_10 = (content.filter(lambda line: line.split("\t")[7] != "NULL")
 .map(lambda line: (line.split("\t")[1], float(line.split("\t")[7])))
                     .takeOrdered(10, lambda x : -x[1])
                    )
```


```python
name_weight_all_records = (content.filter(lambda line: line.split("\t")[7] != "NULL").
map(lambda line: (line.split("\t")[1], float(line.split("\t")[7]))))
```


```python
name_weight_all_records.filter(lambda line: line not in top_10).takeOrdered(15, lambda x : -x[1])
```




    [('Mountain-500 Silver, 52', 13008.96),
     ('Mountain-500 Black, 52', 13008.96),
     ('Mountain-500 Silver, 48', 12891.03),
     ('Mountain-500 Black, 48', 12891.03),
     ('Mountain-500 Silver, 44', 12759.49),
     ('Mountain-500 Black, 44', 12759.49),
     ('Touring-2000 Blue, 60', 12655.16),
     ('Mountain-500 Silver, 42', 12596.19),
     ('Mountain-500 Black, 42', 12596.19),
     ('Touring-2000 Blue, 54', 12555.37),
     ('Touring-2000 Blue, 50', 12437.44),
     ('Mountain-400-W Silver, 46', 12437.44),
     ('Mountain-500 Silver, 40', 12405.69),
     ('Mountain-500 Black, 40', 12405.69),
     ('Touring-2000 Blue, 46', 12305.9)]



**DataFrame way**


```python
df = sqlContext.createDataFrame(name_weight_all_records, schema = ["Name", "Weight"])
```


```python
top_10 = df.orderBy("Weight", ascending = False).take(10)
```


```python
top_10_names = [x[0] for x in top_10]
top_10_weights = [x[1] for x in top_10]
```


```python
from pyspark.sql.functions import col
```


```python
(df.filter((~col("Name").isin(top_10_names)) & (~col("Weight").isin(top_10_names)))
.orderBy("Weight", ascending = False)
.show(15, truncate = False)
)
```

    +-------------------------+--------+
    |Name                     |Weight  |
    +-------------------------+--------+
    |Mountain-500 Black, 52   |13008.96|
    |Mountain-500 Silver, 52  |13008.96|
    |Mountain-500 Silver, 48  |12891.03|
    |Mountain-500 Black, 48   |12891.03|
    |Mountain-500 Silver, 44  |12759.49|
    |Mountain-500 Black, 44   |12759.49|
    |Touring-2000 Blue, 60    |12655.16|
    |Mountain-500 Silver, 42  |12596.19|
    |Mountain-500 Black, 42   |12596.19|
    |Touring-2000 Blue, 54    |12555.37|
    |Mountain-400-W Silver, 46|12437.44|
    |Touring-2000 Blue, 50    |12437.44|
    |Mountain-500 Silver, 40  |12405.69|
    |Mountain-500 Black, 40   |12405.69|
    |Touring-2000 Blue, 46    |12305.9 |
    +-------------------------+--------+
    only showing top 15 rows
    


As of now, I think Spark SQL does not support OFFSET.

**3. Retrieve product details for products where the product model ID is 1**

**RDD way**

Let's display the Name, Color, Size and product model 


```python
(content.filter(lambda line:line.split("\t")[9]=="1")
 .map(lambda line: (line.split("\t")[1],line.split("\t")[3], line.split("\t")[6], line.split("\t")[9])).collect()
)
```




    [('Classic Vest, S', 'Blue', 'S', '1'),
     ('Classic Vest, M', 'Blue', 'M', '1'),
     ('Classic Vest, L', 'Blue', 'L', '1')]



**DataFrame way**


```python
rdd = content.map(lambda line: (line.split("\t")[1],line.split("\t")[3], line.split("\t")[6], line.split("\t")[9])).collect()
```


```python
df = sqlContext.createDataFrame(rdd, schema = ["Name", "Color", "Size","ProductModelID"])
```


```python
df.filter(df["ProductModelID"]==1).show()
```

    +---------------+-----+----+--------------+
    |           Name|Color|Size|ProductModelID|
    +---------------+-----+----+--------------+
    |Classic Vest, S| Blue|   S|             1|
    |Classic Vest, M| Blue|   M|             1|
    |Classic Vest, L| Blue|   L|             1|
    +---------------+-----+----+--------------+
    


** Running SQL Queries Programmatically**


```python
df.createOrReplaceTempView("df_table")
sqlContext.sql(" SELECT * FROM df_table  WHERE ProductModelID = 1").show()
```

    +---------------+-----+----+--------------+
    |           Name|Color|Size|ProductModelID|
    +---------------+-----+----+--------------+
    |Classic Vest, S| Blue|   S|             1|
    |Classic Vest, M| Blue|   M|             1|
    |Classic Vest, L| Blue|   L|             1|
    +---------------+-----+----+--------------+
    


** 4. Retrieve the product number and name of the products that have a color of 'black', 'red', or 'white' and a size of 'S' or 'M'** 

**RDD way**


```python
colors = ["White","Black","Red"]
sizes = ["S","M"]

(content.filter(lambda line: line.split("\t")[6] in sizes)
.filter(lambda line: line.split("\t")[3] in colors)
.map(lambda line: (line.split("\t")[1],line.split("\t")[2], line.split("\t")[3],line.split("\t")[6]))
 .collect()
)
```




    [('Mountain Bike Socks, M', 'SO-B909-M', 'White', 'M'),
     ("Men's Sports Shorts, S", 'SH-M897-S', 'Black', 'S'),
     ("Men's Sports Shorts, M", 'SH-M897-M', 'Black', 'M'),
     ("Women's Tights, S", 'TG-W091-S', 'Black', 'S'),
     ("Women's Tights, M", 'TG-W091-M', 'Black', 'M'),
     ('Half-Finger Gloves, S', 'GL-H102-S', 'Black', 'S'),
     ('Half-Finger Gloves, M', 'GL-H102-M', 'Black', 'M'),
     ('Full-Finger Gloves, S', 'GL-F110-S', 'Black', 'S'),
     ('Full-Finger Gloves, M', 'GL-F110-M', 'Black', 'M'),
     ("Women's Mountain Shorts, S", 'SH-W890-S', 'Black', 'S'),
     ("Women's Mountain Shorts, M", 'SH-W890-M', 'Black', 'M'),
     ('Racing Socks, M', 'SO-R809-M', 'White', 'M')]



**DataFrame way**


```python
rdd = content.map(lambda line: (line.split("\t")[1],line.split("\t")[2], line.split("\t")[3],line.split("\t")[6])).collect()
df = sqlContext.createDataFrame(rdd, schema = ["Name","ProductNumber","Color", "Size"])
```


```python
colors = ["White","Black","Red"]
sizes = ["S","M"]
df.filter(col("Color").isin(colors) & col("Size").isin(sizes)).show()
```

    +--------------------+-------------+-----+----+
    |                Name|ProductNumber|Color|Size|
    +--------------------+-------------+-----+----+
    |Mountain Bike Soc...|    SO-B909-M|White|   M|
    |Men's Sports Shor...|    SH-M897-S|Black|   S|
    |Men's Sports Shor...|    SH-M897-M|Black|   M|
    |   Women's Tights, S|    TG-W091-S|Black|   S|
    |   Women's Tights, M|    TG-W091-M|Black|   M|
    |Half-Finger Glove...|    GL-H102-S|Black|   S|
    |Half-Finger Glove...|    GL-H102-M|Black|   M|
    |Full-Finger Glove...|    GL-F110-S|Black|   S|
    |Full-Finger Glove...|    GL-F110-M|Black|   M|
    |Women's Mountain ...|    SH-W890-S|Black|   S|
    |Women's Mountain ...|    SH-W890-M|Black|   M|
    |     Racing Socks, M|    SO-R809-M|White|   M|
    +--------------------+-------------+-----+----+
    


** Running SQL Queries Programmatically**


```python
df.createOrReplaceTempView("df_table")
sqlContext.sql(" SELECT * FROM df_table  WHERE Color IN ('White','Black','Red') AND Size IN ('S','M')").show(truncate = False)
```

    +--------------------------+-------------+-----+----+
    |Name                      |ProductNumber|Color|Size|
    +--------------------------+-------------+-----+----+
    |Mountain Bike Socks, M    |SO-B909-M    |White|M   |
    |Men's Sports Shorts, S    |SH-M897-S    |Black|S   |
    |Men's Sports Shorts, M    |SH-M897-M    |Black|M   |
    |Women's Tights, S         |TG-W091-S    |Black|S   |
    |Women's Tights, M         |TG-W091-M    |Black|M   |
    |Half-Finger Gloves, S     |GL-H102-S    |Black|S   |
    |Half-Finger Gloves, M     |GL-H102-M    |Black|M   |
    |Full-Finger Gloves, S     |GL-F110-S    |Black|S   |
    |Full-Finger Gloves, M     |GL-F110-M    |Black|M   |
    |Women's Mountain Shorts, S|SH-W890-S    |Black|S   |
    |Women's Mountain Shorts, M|SH-W890-M    |Black|M   |
    |Racing Socks, M           |SO-R809-M    |White|M   |
    +--------------------------+-------------+-----+----+
    


** 5. Retrieve the product number, name, and list price of products whose product number begins with 'BK-'**

**RDD way**


```python
(content.filter(lambda line: "BK" in line.split("\t")[2])
 .map(lambda line: (line.split("\t")[1],line.split("\t")[2], line.split("\t")[3],float(line.split("\t")[5])))
.takeOrdered(10, lambda x: -x[3]))   # Displaying the heaviest 10
```




    [('Road-150 Red, 62', 'BK-R93R-62', 'Red', 3578.27),
     ('Road-150 Red, 44', 'BK-R93R-44', 'Red', 3578.27),
     ('Road-150 Red, 48', 'BK-R93R-48', 'Red', 3578.27),
     ('Road-150 Red, 52', 'BK-R93R-52', 'Red', 3578.27),
     ('Road-150 Red, 56', 'BK-R93R-56', 'Red', 3578.27),
     ('Mountain-100 Silver, 38', 'BK-M82S-38', 'Silver', 3399.99),
     ('Mountain-100 Silver, 42', 'BK-M82S-42', 'Silver', 3399.99),
     ('Mountain-100 Silver, 44', 'BK-M82S-44', 'Silver', 3399.99),
     ('Mountain-100 Silver, 48', 'BK-M82S-48', 'Silver', 3399.99),
     ('Mountain-100 Black, 38', 'BK-M82B-38', 'Black', 3374.99)]



** DataFrame way**


```python
rdd = content.map(lambda line: (line.split("\t")[1],line.split("\t")[2], line.split("\t")[3],float(line.split("\t")[5])))

df = sqlContext.createDataFrame(rdd, schema = ["Name","ProductNumber","Color", "ListPrice"])
```

Here, we can use the **re** python module with the PySpark's User Defined Functions (udf).


```python
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

import re

def is_match(line):
    pattern = re.compile("^(BK-)")
    return(bool(pattern.match(line)))

filter_udf = udf(is_match, BooleanType())

df.filter(filter_udf(df.ProductNumber)).orderBy("ListPrice", ascending = False).show(10, truncate = False)
```

    +-----------------------+-------------+------+---------+
    |Name                   |ProductNumber|Color |ListPrice|
    +-----------------------+-------------+------+---------+
    |Road-150 Red, 44       |BK-R93R-44   |Red   |3578.27  |
    |Road-150 Red, 62       |BK-R93R-62   |Red   |3578.27  |
    |Road-150 Red, 52       |BK-R93R-52   |Red   |3578.27  |
    |Road-150 Red, 56       |BK-R93R-56   |Red   |3578.27  |
    |Road-150 Red, 48       |BK-R93R-48   |Red   |3578.27  |
    |Mountain-100 Silver, 48|BK-M82S-48   |Silver|3399.99  |
    |Mountain-100 Silver, 44|BK-M82S-44   |Silver|3399.99  |
    |Mountain-100 Silver, 42|BK-M82S-42   |Silver|3399.99  |
    |Mountain-100 Silver, 38|BK-M82S-38   |Silver|3399.99  |
    |Mountain-100 Black, 44 |BK-M82B-44   |Black |3374.99  |
    +-----------------------+-------------+------+---------+
    only showing top 10 rows
    


** Running SQL Queries Programmatically**


```python
df.createOrReplaceTempView("df_table")
sqlContext.sql(" SELECT * FROM df_table  WHERE ProductNumber LIKE 'BK-%' ORDER BY ListPrice DESC ").show(n = 10)
```

    +--------------------+-------------+------+---------+
    |                Name|ProductNumber| Color|ListPrice|
    +--------------------+-------------+------+---------+
    |    Road-150 Red, 44|   BK-R93R-44|   Red|  3578.27|
    |    Road-150 Red, 62|   BK-R93R-62|   Red|  3578.27|
    |    Road-150 Red, 52|   BK-R93R-52|   Red|  3578.27|
    |    Road-150 Red, 56|   BK-R93R-56|   Red|  3578.27|
    |    Road-150 Red, 48|   BK-R93R-48|   Red|  3578.27|
    |Mountain-100 Silv...|   BK-M82S-48|Silver|  3399.99|
    |Mountain-100 Silv...|   BK-M82S-44|Silver|  3399.99|
    |Mountain-100 Silv...|   BK-M82S-42|Silver|  3399.99|
    |Mountain-100 Silv...|   BK-M82S-38|Silver|  3399.99|
    |Mountain-100 Blac...|   BK-M82B-44| Black|  3374.99|
    +--------------------+-------------+------+---------+
    only showing top 10 rows
    


** 6. Modify your previous query to retrieve the product number, name, and list price of products whose product number begins 'BK-' followed by any character other than 'R’, and ends with a '-' followed by any two numerals.**


```python
def is_match(line):
    pattern = re.compile("^(BK-)[^R]+(-\d{2})$")
    return(bool(pattern.match(line)))
```

Let's check our function.


```python
is_match("BK-M82S-38")
```




    True



**RDD way**


```python
(content.filter(lambda line: is_match(line.split("\t")[2]))
.map(lambda line: (line.split("\t")[1],line.split("\t")[2], line.split("\t")[3],float(line.split("\t")[5])))
 .takeOrdered(10, lambda x: -x[3]))   # Displaying the heaviest 10
```




    [('Mountain-100 Silver, 38', 'BK-M82S-38', 'Silver', 3399.99),
     ('Mountain-100 Silver, 42', 'BK-M82S-42', 'Silver', 3399.99),
     ('Mountain-100 Silver, 44', 'BK-M82S-44', 'Silver', 3399.99),
     ('Mountain-100 Silver, 48', 'BK-M82S-48', 'Silver', 3399.99),
     ('Mountain-100 Black, 38', 'BK-M82B-38', 'Black', 3374.99),
     ('Mountain-100 Black, 42', 'BK-M82B-42', 'Black', 3374.99),
     ('Mountain-100 Black, 44', 'BK-M82B-44', 'Black', 3374.99),
     ('Mountain-100 Black, 48', 'BK-M82B-48', 'Black', 3374.99),
     ('Touring-1000 Yellow, 46', 'BK-T79Y-46', 'Yellow', 2384.07),
     ('Touring-1000 Yellow, 50', 'BK-T79Y-50', 'Yellow', 2384.07)]



**DataFrame way**


```python
filter_udf = udf(is_match, BooleanType())

df.filter(filter_udf(df.ProductNumber)).orderBy("ListPrice", ascending = False).show(10, truncate = False)
```

    +-----------------------+-------------+------+---------+
    |Name                   |ProductNumber|Color |ListPrice|
    +-----------------------+-------------+------+---------+
    |Mountain-100 Silver, 44|BK-M82S-44   |Silver|3399.99  |
    |Mountain-100 Silver, 48|BK-M82S-48   |Silver|3399.99  |
    |Mountain-100 Silver, 38|BK-M82S-38   |Silver|3399.99  |
    |Mountain-100 Silver, 42|BK-M82S-42   |Silver|3399.99  |
    |Mountain-100 Black, 42 |BK-M82B-42   |Black |3374.99  |
    |Mountain-100 Black, 48 |BK-M82B-48   |Black |3374.99  |
    |Mountain-100 Black, 44 |BK-M82B-44   |Black |3374.99  |
    |Mountain-100 Black, 38 |BK-M82B-38   |Black |3374.99  |
    |Touring-1000 Blue, 54  |BK-T79U-54   |Blue  |2384.07  |
    |Touring-1000 Blue, 50  |BK-T79U-50   |Blue  |2384.07  |
    +-----------------------+-------------+------+---------+
    only showing top 10 rows
    


#### This is enough for today. See you in the next part of the DataFrames Vs RDDs in Spark tutorial series.
