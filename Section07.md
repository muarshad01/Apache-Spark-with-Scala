## 53 - Introducing MLLib
* The previous API was called `MLLlib` and used RDDs and some specialized data structures
* `MLLib` is deprecated in Spark 3
* The newer ML library just used `DataFrames` for everything

***

## 54 - [Activity] Using MLLib to Produce Movie Recommendations

* File `MovieRecommendationsALSDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.collection.mutable

object MovieRecommendationsALSDataset {

  case class MoviesNames(movieId: Int, movieTitle: String)
  // Row format to feed into ALS
  case class Rating(userID: Int, movieID: Int, rating: Float)

  // Get movie name by given dataset and id
  def getMovieName(movieNames: Array[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(_.movieId == movieId)(0)

    result.movieTitle
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Make a session
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()

    
    println("Loading movie names...")
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

    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val names = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    val namesList = names.collect()

    // Load up movie data as dataset
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Rating]
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")
    
    val model = als.fit(ratings)
      
    // Get top-10 recommendations for the user we specified
    val userID:Int = args(0).toInt
    val users = Seq(userID).toDF("userID")
    val recommendations = model.recommendForUserSubset(users, 10)
    
    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")

    for (userRecs <- recommendations) {
      val myRecs = userRecs(1) // First column is userID, second is the recs
      val temp = myRecs.asInstanceOf[mutable.WrappedArray[Row]] // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = getMovieName(namesList, movie)
        println(movieName, rating)
      }
    }
    
    // Stop the session
    spark.stop()
  }
}
```
***

## 55 - Linear Regression with MLLib

***

## 56 - [Activity] Running a Linear Regression with Spark
* File `LinearRegressionDataFrameDataset.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._

object LinearRegressionDataFrameDataset {
  
  case class RegressionSchema(label: Double, features_raw: Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()
      
    // Load up our page speed / amount spent data in the format required by MLLib
    // (which is label, vector of features)
      
    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.
    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)
      .add("features_raw", DoubleType, nullable = true)

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .schema(regressionSchema)
      .csv("data/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("features_raw")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
        .select("label","features")
     
    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Now create our linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.3) // regularization 
      .setElasticNetParam(0.8) // elastic net mixing
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance
    
    // Train the model using our training data
    val model = lir.fit(trainingDF)
    
    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our 
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()
    
    // This basically adds a "prediction" column to our testDF dataframe.
    
    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").collect()
    
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop the session
    spark.stop()
  }
}
```
***

## 57 - [Exercise] Predict Real Estate Values with Decision Trees in Spark

***

## 58 - Exercise Solution: Predicting Real Estate with Decision Trees in Spark
Quiz 5: Quiz: Spark ML
