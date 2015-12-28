import scala.util.matching.Regex

// Create the Regex
val pattern = new Regex("(A|a)msterdam")

// Create a string for testing
val str = "One of my favorite cities is Amsterdam. aaamsterdam!"

println((pattern findAllIn str).mkString(","))
