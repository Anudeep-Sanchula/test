import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class RvPotentialConditionsGapUpdater {

  def ReadPipeDelimitedFileFromS3(): Unit = {
    val sparkConf = new SparkConf().setAppName("ReadPipeDelimitedFileFromS3")
    val sc: SparkContext = new SparkContext(sparkConf)
    val glueContext: GlueContext = new GlueContext(sc)

    val s3Path = "s3://testrawbucket123/raw/test.txt"

    val df = glueContext.sparkSession.read
      .format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(s3Path)

    val payerLobDf = df.select("PAYER", "LOB")
    val firstRow = payerLobDf.first()
    val firstPayer = firstRow.getString(0)
    val firstLob = firstRow.getString(1)
    
    WriteDataFrameToParquet(df, s"s3://testrawbucket123/processed/$firstPayer/$firstLob/")
  }

  def WriteDataFrameToParquet(df: org.apache.spark.sql.DataFrame, outputPath: String): Unit = {
    df.write
      .format("parquet")
      .mode("overwrite")
      .save(outputPath)

    println(s"DataFrame successfully written to Parquet at $outputPath")
  }
}

object RvPotentialConditionsGapUpdaterJob {
  def main(args: Array[String]): Unit = {
    val updater = new RvPotentialConditionsGapUpdater()
    updater.ReadPipeDelimitedFileFromS3()
  }
}
