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

***

## 29 - [Activity] Word Count example, using Datasets

***

## 30 - [Activity] Revisiting the Minimum Temperature example, with Datasets

***

## 31 - [Exercise] Implement the "Total Spent by Customer" problem with Datasets

***

## 32 - Exercise Solution: Total Spent by Customer with Datasets
* Quiz 3: Quiz: SparkSQL
