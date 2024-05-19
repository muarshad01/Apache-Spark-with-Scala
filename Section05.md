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

* File `PopularMoviesNicerDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesNicerDataset {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("PopularMoviesNicer")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // Load up movie data as dataset
    import spark.implicits._
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    // Get number of reviews per movieID
    val movieCounts = movies.groupBy("movieID").count()

    // Create a user-defined function to look up movie names from our
    // shared Map variable.

    // We start by declaring an "anonymous function" in Scala
    val lookupName : Int => String = (movieID:Int)=>{
      nameDict.value(movieID)
    }

    // Then wrap it with a udf
    val lookupNameUDF = udf(lookupName)

    // Add a movieTitle column using our new udf
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    // Sort the results
    val sortedMoviesWithNames = moviesWithNames.sort("count")

    // Show the results without truncating it
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)
  }
}
```

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
