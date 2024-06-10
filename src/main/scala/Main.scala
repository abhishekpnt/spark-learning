import org.apache.spark.sql.SparkSession

object SparkExample {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder
      .appName("Spark Scala Example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Read the text file
    val textFile = spark.read.textFile("src/main/resources/input.txt")

    // Show the contents of the text file
    textFile.show()

    // Count the number of lines in the text file
    val lineCount = textFile.count()
    println(s"Number of lines: $lineCount")

    // Perform a transformation and action
    val words = textFile.flatMap(line => line.split(" "))
//      .toDS()
    val wordCounts = words.groupByKey(identity).count()
    wordCounts.show()

    // Stop the Spark session
    spark.stop()
  }
}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkContext
//object SparkDemo {
//  def main(args : Array[String]): Unit ={
//    Logger.getRootLogger.setLevel(Level.INFO)
//    val sc = new SparkContext("local[*]" , "SparkDemo")
//    val lines = sc.textFile("src/main/resources/input.txt");
//    val words = lines.flatMap(line => line.split(' '))
//    val wordsKVRdd = words.map(x => (x,1))
//    val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
//    count.foreach(println)
//  }
//}