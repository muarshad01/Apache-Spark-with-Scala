## 01 - Udemy 101: Getting the Most From This Course

***

## 02 - Alternate download link for the ml-100k dataset

***

## 03 - WARNING: DO NOT INSTALL JAVA 16 IN THE NEXT LECTURE

***

## 04 - Introduction, and installing the course material, `IntelliJ`, and Scala
* https://www.sundog-education.com/sparkscala/

```scala
package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val lines = sc.textFile("data/ml-100k/u.data")
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    sc.stop()
  }
}
```

***

## 05 - Introduction to Apache Spark
* Quiz 1: Spark Basics

***

## 06 - Important note

***
