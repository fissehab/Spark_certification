
### Leveraging Hive with Spark using Python

In this blog post, we will see how to use Spark with Hive, particularly:
    - how to create and use Hive databases
    - how to create and use Hive tables
    - how to load data to Hive tables
    - how to insert data to Hive tables
    - how to read data from Hive tables
    - we will also see how to save dataframes to any Hadoop supported file system

To work with hive, we have to instantiate SparkSession with Hive support, including connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions if we are using Spark 2.0.0 and later. If we are using earleir Spark versions, we have to use **HiveContext**  which is variant of Spark SQL that integrates with data stored in Hive. Even when we do not have an existing Hive deployment, we can still enable Hive support. 

In this tutorial, I am using stand alone Spark. When not configured by the hive-site.xml, the context automatically creates metastore_db in the current directory. As shown below, initially, we do not have metastore_db but after we instantiate SparkSession with Hive support, we see that metastore_db has been created. Further, when we excute create database command, **spark-warehouse** is created.

First, let's see what we have in the current working directory.


```python
import os
os.listdir(os.getcwd())
```




    ['Leveraging Hive with Spark using Python.ipynb',
     'derby.log']



Initially, we do not have **metastore_db**.


```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
```

Now, let's check if metastore_db  has been created.


```python
os.listdir(os.getcwd())
```




    ['Leveraging Hive with Spark using Python.ipynb',
     'metastore_db',
     '.ipynb_checkpoints',
     'derby.log']



Now, as you can see above, **metastore_db** has been created.

Now, we can use Hive commands to see databases and tables. However, at this point, we do not have any database or table. We will create them below.


```python
spark.sql('show databases').show()
```

    +------------+
    |databaseName|
    +------------+
    |     default|
    +------------+
    



```python
spark.sql('show tables').show()
```

    +--------+---------+-----------+
    |database|tableName|isTemporary|
    +--------+---------+-----------+
    +--------+---------+-----------+
    


We can see the functions in Spark.SQL using the command below. At the time of this writing, we have about 250 functions. 


```python
fncs =  spark.sql('show functions').collect()
len(fncs)
```




    252



Let's see some of them.


```python
for i in fncs[100:111]:
    print(i[0])
```

    initcap
    inline
    inline_outer
    input_file_block_length
    input_file_block_start
    input_file_name
    instr
    int
    isnan
    isnotnull
    isnull


By the way, we can see what a function is used for and what the arguments are as below.


```python
spark.sql("describe function instr").show(truncate = False)
```

    +-----------------------------------------------------------------------------------------------------+
    |function_desc                                                                                        |
    +-----------------------------------------------------------------------------------------------------+
    |Function: instr                                                                                      |
    |Class: org.apache.spark.sql.catalyst.expressions.StringInstr                                         |
    |Usage: instr(str, substr) - Returns the (1-based) index of the first occurrence of `substr` in `str`.|
    +-----------------------------------------------------------------------------------------------------+
    


Now, let's create a database. The data we will use is [MovieLens 20M Dataset](http://files.grouplens.org/datasets/movielens/). We will use movies, ratings and tags data sets.


```python
spark.sql('create database movies')
```




    DataFrame[]



Let's check if our database has been created.


```python
spark.sql('show databases').show()
```

    +------------+
    |databaseName|
    +------------+
    |     default|
    |      movies|
    +------------+
    


Yes, movies database has been created.

Now, let's download the data. I am using Jupyter Notebook so **!** enabes me to use shell commands.


```python
! wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
```

    --2018-01-10 22:07:23--  http://files.grouplens.org/datasets/movielens/ml-latest.zip
    Resolving files.grouplens.org (files.grouplens.org)... 128.101.34.235
    Connecting to files.grouplens.org (files.grouplens.org)|128.101.34.235|:80... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 248434223 (237M) [application/zip]
    Saving to: ‘ml-latest.zip’
    
    ml-latest.zip       100%[===================>] 236.92M  1.02MB/s    in 2m 40s  
    
    2018-01-10 22:10:04 (1.48 MB/s) - ‘ml-latest.zip’ saved [248434223/248434223]
    


Now, let's create tables: in textfile format, in ORC and in AVRO format. But first, we have to make sure we are using the movies database by switching to it using the command below.


```python
spark.sql('use movies')
```




    DataFrame[]



The movies dataset has movieId, title and genres fields. The ratings dataset, on the other hand, as userId, movieID, rating and timestamp fields. Now, let's create the tables.


Please refer to the [Hive manual](http://files.grouplens.org/datasets/movielens/ml-latest.zip) for details on how to create tables and load/insert data into the tables.


```python
spark.sql('create table movies \
         (movieId int,title string,genres string) \
         row format delimited fields terminated by ","\
         stored as textfile')                                              # in textfile format
```


```python
spark.sql("create table ratings\
           (userId int,movieId int,rating float,timestamp string)\
           stored as ORC" )                                                # in ORC format
```




    DataFrame[]



Let's create another table in AVRO format. We will insert count of movies by generes into it later.


```python
spark.sql("create table genres_by_count\
           ( genres string,count int)\
           stored as AVRO" )                                               # in AVRO format
```




    DataFrame[]



Now, let's see if the tables have been created.


```python
spark.sql("show tables").show()
```

    +--------+---------------+-----------+
    |database|      tableName|isTemporary|
    +--------+---------------+-----------+
    |  movies|genres_by_count|      false|
    |  movies|         movies|      false|
    |  movies|        ratings|      false|
    |  movies|           tags|      false|
    +--------+---------------+-----------+
    


We see all the tables we created above.

We can get information about a table as below. If we do not include formatted or extended in the command, we see only information about the columns. But now, we see even its location, the database and other attributes. 


```python
spark.sql("describe formatted ratings").show(truncate = False)
```

    +----------------------------+-------------------------------------------------------------------+-------+
    |col_name                    |data_type                                                          |comment|
    +----------------------------+-------------------------------------------------------------------+-------+
    |userId                      |int                                                                |null   |
    |movieId                     |int                                                                |null   |
    |rating                      |float                                                              |null   |
    |timestamp                   |string                                                             |null   |
    |                            |                                                                   |       |
    |# Detailed Table Information|                                                                   |       |
    |Database                    |movies                                                             |       |
    |Table                       |ratings                                                            |       |
    |Owner                       |fish                                                               |       |
    |Created                     |Thu Jan 11 20:28:31 EST 2018                                       |       |
    |Last Access                 |Wed Dec 31 19:00:00 EST 1969                                       |       |
    |Type                        |MANAGED                                                            |       |
    |Provider                    |hive                                                               |       |
    |Table Properties            |[transient_lastDdlTime=1515720511]                                 |       |
    |Location                    |file:/home/fish/MySpark/HiveSpark/spark-warehouse/movies.db/ratings|       |
    |Serde Library               |org.apache.hadoop.hive.ql.io.orc.OrcSerde                          |       |
    |InputFormat                 |org.apache.hadoop.hive.ql.io.orc.OrcInputFormat                    |       |
    |OutputFormat                |org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat                   |       |
    |Storage Properties          |[serialization.format=1]                                           |       |
    |Partition Provider          |Catalog                                                            |       |
    +----------------------------+-------------------------------------------------------------------+-------+
    


Now let's load data to the movies table. We can load data from a local file system or from any hadoop supported file system. If we are using a hadoop directory, we have to remove **local** from the command below. Please refer the [hive manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Loadingfilesintotables) for details. If we are loading it just one time, we do not need to include **overwrite**. However, if there is possiblity that we could run the code more than one time, including **overwrite** is important not to append the same dataset to the table again and again. Hive does not do any transformation while loading data into tables. Load operations are currently pure copy/move operations that move datafiles into locations corresponding to Hive tables. Hive does some minimal checks to make sure that the files being loaded match the target table. So, pay careful attention to your code.


```python
spark.sql("load data local inpath '/home/fish/MySpark/HiveSpark/movies.csv'\
                 overwrite into table movies")
```




    DataFrame[]



Rather than loading the data as a bulk, we can pre-process it and create a dataframe and insert our dataframe to the table. Let's insert the ratings data by first creating a dataframe.

We can create dataframes in two ways.
- by using the Spark SQL read function such as spark.read.csv, spark.read.json, spark.read.orc, spark.read.avro, spark.rea.parquet, etc.
- by reading it in as an RDD and converting it to a dataframe after pre-processing it

Let's specify schema for the ratings dataset. 


```python
from pyspark.sql.types import *

schema = StructType([
             StructField('userId', IntegerType()),
             StructField('movieId', IntegerType()),
             StructField('rating', DoubleType()),
             StructField('timestamp', StringType())
            ])
```

Now, we can read it in as dataframe using dataframe reader as below.


```python
ratings_df = spark.read.csv("/home/fish/MySpark/HiveSpark/ratings.csv", schema = schema, header = True)
```

We can see the schema of the dataframe as:


```python
ratings_df.printSchema()
```

    root
     |-- userId: integer (nullable = true)
     |-- movieId: integer (nullable = true)
     |-- rating: double (nullable = true)
     |-- timestamp: string (nullable = true)
    


We can also display the first five records from the dataframe.


```python
ratings_df.show(5)
```

    +------+-------+------+-------------+
    |userId|movieId|rating|    timestamp|
    +------+-------+------+-------------+
    |     1|    110|   1.0|1.425941529E9|
    |     1|    147|   4.5|1.425942435E9|
    |     1|    858|   5.0|1.425941523E9|
    |     1|   1221|   5.0|1.425941546E9|
    |     1|   1246|   5.0|1.425941556E9|
    +------+-------+------+-------------+
    only showing top 5 rows
    


The second option to create a dataframe is to read it in as RDD and change it to dataframe by using the **toDF** dataframe function or createDataFrame from SparkSession . Remember, we have to use the **Row** function from pyspark.sql to use **toDF**.


```python
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

rdd = sc.textFile("/home/fish/MySpark/HiveSpark/ratings.csv")
header = rdd.first()
ratings_df2 = rdd.filter(lambda line: line != header).map(lambda line: Row(userId = int(line.split(",")[0]),
                                                                     movieId = int(line.split(",")[1]),
                                                                     rating = float(line.split(",")[2]),
                                                                     timestamp = line.split(",")[3]
                                                                    )).toDF()
            
```

We can also do as below:


```python
rdd2 = rdd.filter(lambda line: line != header).map(lambda line:line.split(","))
ratings_df2_b =spark.createDataFrame(rdd2, schema = schema) 
```

We see the schema and the the first five records from ratings_df and ratings_df2 are the same.


```python
ratings_df2.printSchema()
```

    root
     |-- movieId: long (nullable = true)
     |-- rating: double (nullable = true)
     |-- timestamp: string (nullable = true)
     |-- userId: long (nullable = true)
    



```python
ratings_df2.show(5)
```

    +-------+------+----------+------+
    |movieId|rating| timestamp|userId|
    +-------+------+----------+------+
    |    110|   1.0|1425941529|     1|
    |    147|   4.5|1425942435|     1|
    |    858|   5.0|1425941523|     1|
    |   1221|   5.0|1425941546|     1|
    |   1246|   5.0|1425941556|     1|
    +-------+------+----------+------+
    only showing top 5 rows
    


To insert a dataframe into a Hive table, we have to first create a temporary table as below. 


```python
ratings_df.createOrReplaceTempView("ratings_df_table") # we can also use registerTempTable
```

Now, let's insert the data to the ratings Hive table.


```python
spark.sql("insert into table ratings select * from ratings_df_table")
```




    DataFrame[]



Next, let's check if the movies and ratings hive tables have the data.


```python
spark.sql("select * from movies limit 10").show(truncate = False)
```

    +-------+----------------------------------+-------------------------------------------+
    |movieId|title                             |genres                                     |
    +-------+----------------------------------+-------------------------------------------+
    |null   |title                             |genres                                     |
    |1      |Toy Story (1995)                  |Adventure|Animation|Children|Comedy|Fantasy|
    |2      |Jumanji (1995)                    |Adventure|Children|Fantasy                 |
    |3      |Grumpier Old Men (1995)           |Comedy|Romance                             |
    |4      |Waiting to Exhale (1995)          |Comedy|Drama|Romance                       |
    |5      |Father of the Bride Part II (1995)|Comedy                                     |
    |6      |Heat (1995)                       |Action|Crime|Thriller                      |
    |7      |Sabrina (1995)                    |Comedy|Romance                             |
    |8      |Tom and Huck (1995)               |Adventure|Children                         |
    |9      |Sudden Death (1995)               |Action                                     |
    +-------+----------------------------------+-------------------------------------------+
    



```python
spark.sql("select * from ratings limit 10").show(truncate = False)
```

    +------+-------+------+-------------+
    |userId|movieId|rating|timestamp    |
    +------+-------+------+-------------+
    |52224 |51662  |3.5   |1.292347002E9|
    |52224 |54286  |4.0   |1.292346944E9|
    |52224 |56367  |3.5   |1.292346721E9|
    |52224 |58559  |4.0   |1.292346298E9|
    |52224 |59315  |3.5   |1.292346497E9|
    |52224 |60069  |4.5   |1.292346644E9|
    |52224 |60546  |4.5   |1.292346916E9|
    |52224 |63082  |4.0   |1.292347049E9|
    |52224 |68157  |3.5   |1.292347351E9|
    |52224 |68358  |4.0   |1.292347043E9|
    +------+-------+------+-------------+
    


We see that we can put our data in hive tables by either directly loading data in a local or hadoop file system or by creating a dataframe and registering the dataframe as a temporary table.

We can also query data in hive table and save it another hive table. Let's calculate number of movies by genres and insert those genres which occur more than   500 times to genres_by_count AVRO hive table we created above.


```python
spark.sql("select genres, count(*) as count from movies\
          group by genres\
          having count(*) > 500 \
          order by count desc").show()
```

    +--------------------+-----+
    |              genres|count|
    +--------------------+-----+
    |               Drama| 5521|
    |              Comedy| 3604|
    |         Documentary| 2903|
    |  (no genres listed)| 2668|
    |        Comedy|Drama| 1494|
    |       Drama|Romance| 1369|
    |      Comedy|Romance| 1017|
    |              Horror|  944|
    |Comedy|Drama|Romance|  735|
    |      Drama|Thriller|  573|
    |         Crime|Drama|  567|
    |     Horror|Thriller|  553|
    |            Thriller|  530|
    +--------------------+-----+
    



```python
spark.sql("insert into table genres_by_count \
          select genres, count(*) as count from movies\
          group by genres\
          having count(*) >= 500 \
          order by count desc")
```




    DataFrame[]



Now, we can check if the data has been inserted to the hive table appropriately:


```python
spark.sql("select * from genres_by_count order by count desc limit 3").show()
```

    +-----------+-----+
    |     genres|count|
    +-----------+-----+
    |      Drama| 5521|
    |     Comedy| 3604|
    |Documentary| 2903|
    +-----------+-----+
    


We can also use data in hive tables with other dataframes by first registering the dataframes as temporary tables.

Now, let's create a temporary table from the tags dataset and then we will join it with movies and ratings tables which are in hive.


```python
schema = StructType([
             StructField('userId', IntegerType()),
             StructField('movieId', IntegerType()),
             StructField('tag', StringType()),
             StructField('timestamp', StringType())
            ])

tags_df = spark.read.csv("/home/fish/MySpark/HiveSpark/tags.csv", schema = schema, header = True)
tags_df.printSchema()
```

    root
     |-- userId: integer (nullable = true)
     |-- movieId: integer (nullable = true)
     |-- tag: string (nullable = true)
     |-- timestamp: string (nullable = true)
    


Next, register the dataframe as temporary table.


```python
tags_df.registerTempTable('tags_df_table')
```

From the **show tables** hive command below, we see that three of them are permanent but two of them are temporary tables.


```python
spark.sql('show tables').show()
```

    +--------+----------------+-----------+
    |database|       tableName|isTemporary|
    +--------+----------------+-----------+
    |  movies| genres_by_count|      false|
    |  movies|          movies|      false|
    |  movies|         ratings|      false|
    |        |ratings_df_table|       true|
    |        |   tags_df_table|       true|
    +--------+----------------+-----------+
    


Now, lets' join the three tables by using inner join. The result is a dataframe.


```python
joined = spark.sql("select m.title, m.genres, r.movieId, r.userId,  r.rating, r.timestamp as ratingTimestamp, \
               t.tag, t.timestamp as tagTimestamp from ratings as r inner join tags_df_table as t\
               on r.movieId = t.movieId and r.userId = t.userId inner join movies as m on r.movieId = m.movieId")
```


```python
type(joined)
```




    pyspark.sql.dataframe.DataFrame



We can see the first five records as below.


```python
joined.select(['title','genres','rating']).show(5, truncate = False)
```

    +-------------------------------------------------------------+----------------------------+------+
    |title                                                        |genres                      |rating|
    +-------------------------------------------------------------+----------------------------+------+
    |Star Wars: Episode IV - A New Hope (1977)                    |Action|Adventure|Sci-Fi     |4.0   |
    |Star Wars: Episode IV - A New Hope (1977)                    |Action|Adventure|Sci-Fi     |4.0   |
    |She Creature (Mermaid Chronicles Part 1: She Creature) (2001)|Fantasy|Horror|Thriller     |2.5   |
    |The Veil (2016)                                              |Horror                      |2.0   |
    |A Conspiracy of Faith (2016)                                 |Crime|Drama|Mystery|Thriller|3.5   |
    +-------------------------------------------------------------+----------------------------+------+
    only showing top 5 rows
    


We  can also save our dataframe in other file system.

Let's create a new directory and save the dataframe in csv, json, orc and parquet formats.

Let's see two ways to do that:


```python
!pwd
```

    /home/fish/MySpark/HiveSpark



```python
!mkdir output
```


```python
joined.write.csv("/home/fish/MySpark/HiveSpark/output/joined.csv", header = True)

joined.write.json("/home/fish/MySpark/HiveSpark/output/joined.json")

joined.write.orc("/home/fish/MySpark/HiveSpark/output/joined_orc")

joined.write.parquet("/home/fish/MySpark/HiveSpark/output/joined_parquet" )
```

Now, let's check if the data is there in the formats we specified.


```python
! ls output
```

    joined.csv  joined.json  joined_orc  joined_parquet


The second option to save data:


```python
joined.write.format('csv').save("/home/fish/MySpark/HiveSpark/output/joined2.csv" , header = True)

joined.write.format('json').save("/home/fish/MySpark/HiveSpark/output/joined2.json" )

joined.write.format('orc').save("/home/fish/MySpark/HiveSpark/output/joined2_orc" )

joined.write.format('parquet').save("/home/fish/MySpark/HiveSpark/output/joined2_parquet" )
```

Now, let's see if we have data from both oprions.


```python
! ls output
```

    joined2.csv   joined2_orc      joined.csv   joined_orc
    joined2.json  joined2_parquet  joined.json  joined_parquet


Similarly, let's see two ways to read the data.

First option:


```python
read_csv = spark.read.csv('/home/fish/MySpark/HiveSpark/output/joined.csv', header = True)

read_orc = spark.read.orc('/home/fish/MySpark/HiveSpark/output/joined_orc')

read_parquet = spark.read.parquet('/home/fish/MySpark/HiveSpark/output/joined_parquet')
```


```python
read_orc.printSchema()
```

    root
     |-- title: string (nullable = true)
     |-- genres: string (nullable = true)
     |-- movieId: integer (nullable = true)
     |-- userId: integer (nullable = true)
     |-- rating: float (nullable = true)
     |-- ratingTimestamp: string (nullable = true)
     |-- tag: string (nullable = true)
     |-- tagTimestamp: double (nullable = true)
    


second option:


```python
read2_csv = spark.read.format('csv').load('/home/fish/MySpark/HiveSpark/output/joined.csv', header = True)

read2_orc = spark.read.format('orc').load('/home/fish/MySpark/HiveSpark/output/joined_orc')

read2_parquet = spark.read.format('parquet').load('/home/fish/MySpark/HiveSpark/output/joined_parquet')
```


```python
read2_parquet.printSchema()
```

    root
     |-- title: string (nullable = true)
     |-- genres: string (nullable = true)
     |-- movieId: integer (nullable = true)
     |-- userId: integer (nullable = true)
     |-- rating: float (nullable = true)
     |-- ratingTimestamp: string (nullable = true)
     |-- tag: string (nullable = true)
     |-- tagTimestamp: double (nullable = true)
    


We can also write a dataframe into a hive table by using **insertInto**. This requires that the schema of the DataFrame is the same as the schema of the table.

Let's see the schema of the **joined** dataframe and create two hive tables: one in ORC and one in PARQUET formats to insert the dataframe into.


```python
joined.printSchema()
```

    root
     |-- title: string (nullable = true)
     |-- genres: string (nullable = true)
     |-- movieId: integer (nullable = true)
     |-- userId: integer (nullable = true)
     |-- rating: float (nullable = true)
     |-- ratingTimestamp: string (nullable = true)
     |-- tag: string (nullable = true)
     |-- tagTimestamp: double (nullable = true)
    


Create ORC Hive Table:


```python
spark.sql("create table joined_orc\
           (title string,genres string, movieId int, userId int,  rating float, \
           ratingTimestamp string,tag string, tagTimestamp string )\
           stored as ORC" )
```




    DataFrame[]



Create PARQUET Hive Table:


```python
spark.sql("create table joined_parquet\
          (title string,genres string, movieId int, userId int,  rating float, \
           ratingTimestamp string,tag string, tagTimestamp string )\
           stored as PARQUET")
```




    DataFrame[]



Let's see if the tables have been created.


```python
spark.sql('show tables').show()
```

    +--------+----------------+-----------+
    |database|       tableName|isTemporary|
    +--------+----------------+-----------+
    |  movies| genres_by_count|      false|
    |  movies|      joined_orc|      false|
    |  movies|  joined_parquet|      false|
    |  movies|          movies|      false|
    |  movies|         ratings|      false|
    |        |ratings_df_table|       true|
    |        |   tags_df_table|       true|
    +--------+----------------+-----------+
    


They are there. Now, let's insert dataframe into the tables.


```python
joined.write.insertInto('joined_orc')
```


```python
joined.write.insertInto('joined_parquet')
```

Finally, let's check if the data has been inserted into the hive tbales.


```python
spark.sql('select title, genres, rating from joined_orc order by rating desc limit 5').show(truncate = False)
```

    +---------------------------+-------------------------------------------+------+
    |title                      |genres                                     |rating|
    +---------------------------+-------------------------------------------+------+
    |To Die For (1995)          |Comedy|Drama|Thriller                      |5.0   |
    |Seven (a.k.a. Se7en) (1995)|Mystery|Thriller                           |5.0   |
    |Seven (a.k.a. Se7en) (1995)|Mystery|Thriller                           |5.0   |
    |Seven (a.k.a. Se7en) (1995)|Mystery|Thriller                           |5.0   |
    |Toy Story (1995)           |Adventure|Animation|Children|Comedy|Fantasy|5.0   |
    +---------------------------+-------------------------------------------+------+
    



```python
spark.sql('select title, genres, rating  from joined_parquet order by rating desc limit 5').show(truncate = False)
```

    +-----------------------------------------+-----------------------+------+
    |title                                    |genres                 |rating|
    +-----------------------------------------+-----------------------+------+
    |Beautiful Girls (1996)                   |Comedy|Drama|Romance   |5.0   |
    |Before Sunrise (1995)                    |Drama|Romance          |5.0   |
    |Beautiful Girls (1996)                   |Comedy|Drama|Romance   |5.0   |
    |Twelve Monkeys (a.k.a. 12 Monkeys) (1995)|Mystery|Sci-Fi|Thriller|5.0   |
    |"Bridges of Madison County               | The (1995)"           |5.0   |
    +-----------------------------------------+-----------------------+------+
    


Everything looks great! See you in my next tutorial on Apache Spark.
