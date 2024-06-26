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

```scala
def transformInt(x : Int, f: Int => Int ) : Int = { f(x) }
val result = transformInt(2, cubeIt)
println(result)
```

```scala
transformInt(3, x => x * x * x)
transformInt(3, x => x / 2)
transformInt(2, x => {val y = x * 2; y * y})

```

***

## 10 - [Exercise] Data Structures in Scala

```scala
// Data Structures

// Tuples
// Immutable lists

val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val picardShip = "Picard" -> "Enterprise-D"
println(picardShip._2)

val aBonusStuff = ("Kirk", 1964, true)
```

```scala
// Lists
// Like a tuple, but more functionality
// Must be of same type

val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Sea")
print(shipList(1))
// zero based

print(shipList.head)
print(shipList.tail)

for (ship <- shipList) { print(ship) }
val backwardShips = shipList.map((ship: string) => {ship.reverse})
for (ship <- shipList) { print(ship) }
```

```scala
// reduce
val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x : Int, y : Int) => x + y)
print(sum)
```

```scala
// filter
val iHateFives = numberList.filter( (x : Int) => x != 5)
val iHateThrees = numberList.filter( _ != 3)
```

```scala

// Concatenate lists
val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers
val reversed =  numberList.reverse
val sorted = reversed.sorted
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum
val hasThree = iHateThrees.contains(3)
```

```scala
// MAPS
val shipMap = Map("Kirk" -> "Enterprise",
                  "Picard" -> "Enterprise-D",
                  "Sisko" -> "Deep Space Nine",
                  "Janeway" -> "Voyager")
print(shipMap("Janeway"))
print(shipMap.contains("Archer"))

val archerShip = util.Try(shipMap("Archer")) getOrElse "UnKnown"
println(archerShip)
```
***
