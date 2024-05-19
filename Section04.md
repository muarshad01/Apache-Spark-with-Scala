## 24 - Introduction to SparkSQL

***

## 25 - [Activity] Using SparkSQL
* File `SparkSQLDataset`
```scala
package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]            # if we comment out `.as[Person]` then schemaPeople is aDataFrame

    # schemaPeople DataSet
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}
```
* Schema has been infered at run-time. That is the main difference between DataFrame and DataSet.

***

## 26 - [Activity] Using DataSets

* File `DataFramesDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFramesDataset {
  
  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    people.select("name").show()
    
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    people.groupBy("age").count().show()
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }
}
```

***

## 27 - [Exercise] Implement the "Friends by Age" example using DataSets

***

## 28 - Exercise Solution: Friends by Age, with Datasets.

* File `FriendsByAgeDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Compute the average number of friends by age in a social network. */
object FriendsByAgeDataset {

  // Create case class with schema of fakefriends.csv
  case class FakeFriends(id: Int, name: String, age: Int, friends: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[FakeFriends]

    // Select only age and numFriends columns
    val friendsByAge = ds.select("age", "friends")

    // From friendsByAge we group by "age" and then compute average
    friendsByAge.groupBy("age").avg("friends").show()

    // Sorted:
    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    // Formatted more nicely:
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()

    // With a custom column name:
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()
  }
}
```

***

## 29 - [Activity] Word Count example, using Datasets
* File `WordCountBetterSortedDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedDataset {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // Split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    // Sort by counts
    val wordCountsSorted = wordCounts.sort("count")

    // Show the results.
    wordCountsSorted.show(wordCountsSorted.count.toInt)


    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }
}
```

***

## 30 - [Activity] Revisiting the Minimum Temperature example, with Datasets

***

## 31 - [Exercise] Implement the "Total Spent by Customer" problem with Datasets

***

## 32 - Exercise Solution: Total Spent by Customer with Datasets
* Quiz 3: Quiz: SparkSQL
