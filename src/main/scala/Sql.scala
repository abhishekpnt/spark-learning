import org.apache.spark.sql.SparkSession
object Sql extends App{

  val spark = SparkSession.builder
    .appName("Spark Scala Example2").master("local")
//    .config("spark.master", "local")
    .getOrCreate()

  // Create DataFrame from a JSON file
  val df = spark.read.json("src/main/resources/file.json")

  // Show the DataFrame
  df.show()

  // Select specific columns
  df.select("email").show()
}
