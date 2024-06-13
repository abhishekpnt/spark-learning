import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object csvTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Examples")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val columns = Array("id", "first", "last", "year")
    val df = spark.sparkContext.parallelize(Seq(
      (1, "John", "Doe", 1986),
      (2, "Ive", "Fish", 1990),
      (4, "John", "Wayne", 1995)
    )).toDF(columns: _*)

    // Coalesce the DataFrame to a single partition
    df.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save("output/singlefile")

    // Get the Hadoop FileSystem
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Get the part file that Spark created
    val outputDir = new Path("output/singlefile")
    val partFile = fs.globStatus(new Path("output/singlefile/part*"))(0).getPath

    // Define the final output file path
    val finalOutputPath = new Path("output/singlefile.csv")

    // Rename the part file to the final output file
    fs.rename(partFile, finalOutputPath)

    // Clean up the original directory
    fs.delete(outputDir, true)

    // Stop the SparkSession
    spark.stop()
  }
}
