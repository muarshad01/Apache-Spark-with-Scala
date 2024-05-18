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

***

## 15 - [Activity] Running the Average Friends by Age Example

***

## 16 - Filtering RDD's, and the Minimum Temperature by Location Example

***

## 17 - [Activity] Running the Minimum Temperature Example, and Modifying it for Maximum

***

## 18 - [Activity] Counting Word Occurrences using Flatmap()

***

## 19 - [Activity] Improving the Word Count Script with Regular Expressions

***

## 20 - [Activity] Sorting the Word Count Results

***

## 21 - [Exercise] Find the Total Amount Spent by Customer

***

## 22 - [Exercise] Check your Results, and Sort Them by Total Amount Spent

***

## 23 - Check Your Results and Implementation Against Mine
* Quiz 2: Quiz: RDD's

***
