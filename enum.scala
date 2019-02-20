// enumeration defining an inner class named value
object Direction extends Enumeration {
   val North, East, Slouth, West = Value
}

// Associate name with enumeration values
object Direction extends Enumeration {
  val North = Value("North")
  val East = Value("East")
  }
  
for (d <- Direction.values) print(d + " ")

Direction.East.id // Int = 1
Direction(1) // Direction.Value = East
