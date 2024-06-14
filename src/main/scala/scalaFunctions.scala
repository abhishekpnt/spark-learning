import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}

object scalaFunctions extends App{




  val spark = SparkSession.builder
    .appName("Spark Scala Example2").master("local")
    .getOrCreate()
  import spark.implicits._
  val sourceDF = Seq(
    ("  p a   b l o", "Paraguay"),
    ("Neymar", "B r    asil")
  ).toDF("name", "country")
println("---",sourceDF)
  val actualDF = Seq(
    "name",
    "country"
  ).foldLeft(sourceDF) { (memoDF, colName) =>
    memoDF.withColumn(
      colName,
      regexp_replace(col(colName), "\\s+", "")
    )
  }

  actualDF.show()
}
