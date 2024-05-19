## 33 - [Activity] Find the Most Popular Movie

* File `PopularMoviesDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/** Find the movies with the most ratings. */
object PopularMoviesDataset {

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    // Load up movie data as dataset
    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]
    
    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count"))

    // Grab the top 10
    topMovieIDs.show(10)

    // Stop the session
    spark.stop()
  }
}
```

***

## 34 - [Activity] Use Broadcast Variables to Display Movie Names

***

## 35 - [Activity] Find the Most Popular Superhero in a Social Graph

***

## 36 - [Exercise] Find the Most Obscure Superheroes

***

## 37 - Exercise Solution: Find the Most Obscure Superheroes

***

## 38 - Superhero Degrees of Separation: Introducing Breadth-First Search

***

## 39 - Superhero Degrees of Separation: Accumulators, and Implementing BFS in Spark

***

## 40 - [Activity] Superhero Degrees of Separation: Review the code, and run it!

***

## 41 - Item-Based Collaborative Filtering in Spark, cache(), and persist()

***

## 42 - [Activity] Running the Similar Movies Script using Spark's Cluster Manager

***

## 43 - [Exercise] Improve the Quality of Similar Movies

***
