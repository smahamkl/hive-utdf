package hive.scala.udtf

object SampleZipIndex {

  def main(args:Array[String]) = {

    val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut")
    println(s"Elements of donuts = $donuts")

    println("\nStep 2: How to zip the donuts Sequence with their corresponding index using zipWithIndex method")
    val zippedDonutsWithIndex: Seq[(String, Int)] = donuts.zipWithIndex

    println(zippedDonutsWithIndex)
  }

}
