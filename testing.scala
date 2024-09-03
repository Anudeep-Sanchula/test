import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class RvPotentialConditionsGapUpdater {
  def ReadPipeDelimitedFileFromS3(): Unit = {
    val sparkConf = new SparkConf().setAppName("ReadPipeDelimitedFileFromS3")
    val sc: SparkContext = new SparkContext(sparkConf)
    val glueContext: GlueContext = new GlueContext(sc)

    val s3Path = "s3://testrawbucket123/raw/test.csv"

    val df = glueContext.sparkSession.read
      .format("csv")
      .option("delimiter", "|")
      .option("header", "true") 
      .load(s3Path)

    df.show()
  }
}

object RvPotentialConditionsGapUpdaterJob {
  def main(args: Array[String]): Unit = {
    val updater = new RvPotentialConditionsGapUpdater()
    updater.ReadPipeDelimitedFileFromS3()
  }
}

#####

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class RvPotentialConditionsGapUpdater {

  def ReadPipeDelimitedFileFromS3(): Unit = {
    val sparkConf = new SparkConf().setAppName("ReadPipeDelimitedFileFromS3")
    val sc: SparkContext = new SparkContext(sparkConf)
    val glueContext: GlueContext = new GlueContext(sc)

    val s3Path = "s3://testrawbucket123/raw/test.csv"

    val df = glueContext.sparkSession.read
      .format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(s3Path)

    df.show()

    WriteDataFrameToParquet(df, "s3://testrawbucket123/processed/")
  }

  // Method to write a DataFrame to a Parquet file in S3
  def WriteDataFrameToParquet(df: org.apache.spark.sql.DataFrame, outputPath: String): Unit = {
    df.write
      .format("parquet") // Specify the format as Parquet
      .mode("overwrite") // Use "overwrite" mode to replace any existing files
      .save(outputPath) // Specify the output path in S3

    println(s"DataFrame successfully written to Parquet at $outputPath")
  }
}

object RvPotentialConditionsGapUpdaterJob {
  def main(args: Array[String]): Unit = {
    val updater = new RvPotentialConditionsGapUpdater()
    updater.ReadPipeDelimitedFileFromS3()
  }
}
