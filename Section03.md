## 11 - The Resilient Distributed Dataset

***

## 12 - Ratings Histogram Example
* File `RatingsCounter.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("data/ml-100k/u.data")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.split("\t")(2))
    
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    
    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)
    
    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
```
***

## 13 - Spark Internals

***

## 14 - Key / Value RDD's, and the Average Friends by Age example
* `(ID, name, age, numFriends)`
* 
***

## 15 - [Activity] Running the Average Friends by Age Example
* File `FriendsByAge.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String): (Int, Int) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (age, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }   
}
```
***

## 16 - Filtering RDD's, and the Minimum Temperature by Location Example

***

## 17 - [Activity] Running the Minimum Temperature Example, and Modifying it for Maximum
* File `MinTemperatures.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("data/1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    
    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
    
    // Collect, format, and print the results
    val results = minTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }     
  }
}
```
***

## 18 - [Activity] Counting Word Occurrences using Flatmap()
* File `WordCount.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("data/book.txt")
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    // Count up the occurrences of each word
    val wordCounts = words.countByValue()
    
    // Print the results.
    wordCounts.foreach(println)
  } 
}
```
***

## 19 - [Activity] Improving the Word Count Script with Regular Expressions

***

## 20 - [Activity] Sorting the Word Count Results
* File `WordCountBetter.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("data/book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()
    
    // Print the results
    wordCounts.foreach(println)
  } 
}
```
***

## 21 - [Exercise] Find the Total Amount Spent by Customer

***

## 22 - [Exercise] Check your Results, and Sort Them by Total Amount Spent
* File `TotalSpentByCustomer.scala`
```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomer {
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")   
    
    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)
    
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
    
    val results = totalByCustomer.collect()
    
    // Print the results.
    results.foreach(println)
  } 
}
```
***

## 23 - Check Your Results and Implementation Against Mine
* Quiz 2: Quiz: RDD's

***
