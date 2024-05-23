## 07 - [Activity] Scala Basics
```scala
// VALUES are immutable constants
val hello: string = hello

// VARIABLES are mutable
var helloThere: string = hello
helloThere = hello + " There!"

val immutableHelloThere = hello + " There"
println(immutableHelloThere)
```

```scala
// Data Types
val numberOne: Int = 1
val truth: Boolean = true
val letterA: char = 'a'
val pi: Double = 3.14159265
val piSinglePrecision: Float = 3.14159265f
val bigNumber: Long = 123456789
val smallNumber: Byte = 127

println(f"Pi is about $piSinglePrecision%.3f")
println(s"I can use the s prefix to use variable like $numberOne $truth $letterA")
```

```scala
val theUltimateAnswer: string = "The life of unvierse and everyting is 42"
val pattern = """.*([\d]+).*""".r
val pattern(answerString) = theUltimateAnswer
val answer = answerString.toInt
print(answer)
```

```scala
val isGreater = 1 > 2
val isLesser = 1 < 2
val impossible = isGreater & isLesser
val anotherWay = isGreater & isLesser
```

***

## 08 - [Exercise] Flow Control in Scala

```scala
val number = 3
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("Something else")
}
```

```scala
for (x <- 1 to 4) {
  squared = x * x
  println (squared)
}
```

***

## 09 - [Exercise] Functions in Scala

```scala
def squareIt(x : Int) : Int = { x * x}
def cubeIt(x : Int) : Int = { x * x * x}

println(squareIt(2))
println(cubeIt(3))
```

***

## 10 - [Exercise] Data Structures in Scala

***
