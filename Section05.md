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

* File `MostPopularSuperheroDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero with the most co-appearances. */
object MostPopularSuperheroDataset {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopular = connections
        .sort($"connections".desc)
        .first()

    val mostPopularName = names
      .filter($"id" === mostPopular(0))
      .select("name")
      .first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")
  }
}
```

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

* File `DegreesOfSeparation.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

/** Finds the degrees of separation between two Marvel comic book characters, based
 *  on co-appearances in a comic.
 */
object DegreesOfSeparation {
  
  // The characters we want to find the separation between.
  val startCharacterID = 5306 //SpiderMan
  val targetCharacterID = 14 //ADAM 3,031 (who?)
  
  // We make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter:Option[LongAccumulator] = None
  
  // Some custom data types 
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, String)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)
    
  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(line: String): BFSNode = {
    
    // Split up the line into fields
    val fields = line.split("\\s+")
    
    // Extract this hero ID from the first field
    val heroID = fields(0).toInt
    
    // Extract subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 until (fields.length - 1)) {
      connections += fields(connection).toInt
    }
    
    // Default distance and color is 9999 and white
    var color:String = "WHITE"
    var distance:Int = 9999
    
    // Unless this is the character we're starting from
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }
    
    (heroID, (connections.toArray, distance, color))
  }
  
  /** Create "iteration 0" of our RDD of BFSNodes */
  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    val inputFile = sc.textFile("data/marvel-graph.txt")
    inputFile.map(convertToBFS)
  }
  
  /** Expands a BFSNode into this node and its children */
  def bfsMap(node:BFSNode): Array[BFSNode] = {
    
    // Extract data from the BFSNode
    val characterID:Int = node._1
    val data:BFSData = node._2
    
    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3
    
    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our new RDD
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()
    
    // Gray nodes are flagged for expansion, and create new
    // gray nodes for each connection
    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"
        
        // Have we stumbled across the character we're looking for?
        // If so increment our accumulator so the driver script knows.
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }
        
        // Create our new Gray node for this connection and add it to the results
        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }
      
      // Color this node as black, indicating it has been processed already.
      color = "BLACK"
    }
    
    // Add the original node back in, so its connections can get merged with 
    // the gray nodes in the reducer.
    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry
    
    results.toArray
  }
  
  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {
    
    // Extract data that we are combining
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3
    
    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()
    
    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }
    
    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }
    
    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
	if (color1 == "GRAY" && color2 == "GRAY") {
	  color = color1
	}
	if (color1 == "BLACK" && color2 == "BLACK") {
	  color = color1
	}
    
    (edges.toArray, distance, color)
  }
    
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DegreesOfSeparation") 
    
    // Our accumulator, used to signal when we find the target 
    // character in our BFS traversal.
    hitCounter = Some(sc.longAccumulator("Hit Counter"))
    
    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)
   
      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap)
      
      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.  
      println("Processing " + mapped.count() + " values.")
      
      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + 
              " different direction(s).")
          return
        }
      }
      
      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.      
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}
```

***

## 41 - Item-Based Collaborative Filtering in Spark, cache(), and persist()

***

## 42 - [Activity] Running the Similar Movies Script using Spark's Cluster Manager

* File `MovieSimilaritiesDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark.sql.functions._
import org.apache.log4j._

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


object MovieSimilaritiesDataset {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)
  case class MoviesNames(movieID: Int, movieTitle: String)
  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)

  def computeCosineSimilarity(spark: SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {
    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("movie1", "movie2", "score", "numPairs").as[MoviePairsSimilarity]

    result
  }

  /** Get movie name by given movie id */
  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(col("movieID") === movieId)
      .select("movieTitle").collect()(0)

    result(0).toString
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\nLoading movie names...")
    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val movieNames = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    // Load up movie data as dataset
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userId", "movieId", "rating")

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    // Select movie pairs and rating pairs
    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId")
      .select($"ratings1.movieId".alias("movie1"),
        $"ratings2.movieId".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).as[MoviePairs]

    val moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = moviePairSimilarities.filter(
        (col("movie1") === movieID || col("movie2") === movieID) &&
          col("score") > scoreThreshold && col("numPairs") > coOccurrenceThreshold)

      // Sort by quality score.
      val results = filteredResults.sort(col("score").desc).take(10)

      println("\nTop 10 similar movies for " + getMovieName(movieNames, movieID))
      for (result <- results) {
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = result.movie1
        if (similarMovieID == movieID) {
          similarMovieID = result.movie2
        }
        println(getMovieName(movieNames, similarMovieID) + "\tscore: " + result.score + "\tstrength: " + result.numPairs)
      }
    }
  }
}
```

***

## 43 - [Exercise] Improve the Quality of Similar Movies

***
