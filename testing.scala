import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CopyObjectRequest, DeleteObjectRequest}


class RvPotentialConditionsGapUpdater {
    def ReadPipeDelimitedFileFromS3(s3Path: String): Unit = {
        val sparkConf = new SparkConf().setAppName("ReadPipeDelimitedFileFromS3")
        val sc: SparkContext = new SparkContext(sparkConf)
        val glueContext: GlueContext = new GlueContext(sc)
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
  

  def CheckIfEmpty(bucketName: String, sourcePrefix: String, destinationPrefix: String): Unit = {
      val s3Client = AmazonS3ClientBuilder.defaultClient()
      val objectListing = s3Client.listObjects(bucketName, sourcePrefix)
      val objectSummaries = objectListing.getObjectSummaries
      
      if (!objectSummaries.isEmpty) {
          println(s"Found ${objectSummaries.size()} objects to move from '$sourcePrefix' to '$destinationPrefix'.")
          
          objectSummaries.forEach { obj =>
            val oldKey = obj.getKey
            val newKey = oldKey.replace(sourcePrefix, destinationPrefix)
            
            println(s"Moving '$oldKey' to '$newKey'.")
            
            s3Client.copyObject(new CopyObjectRequest(bucketName, oldKey, bucketName, newKey))
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, oldKey))
          }
          
          println("All objects have been moved successfully.")
          
      } else {
          println(s"No objects found under '$sourcePrefix' to move.")
      }
  }
  
  def WriteDataFrameToParquet(df: DataFrame, outputPath: String): Unit = {
      CheckIfEmpty("testrawbucket123", "processed", "history")
      df.write
          .format("parquet")
          .mode("overwrite")
          .save(outputPath)
      
      println(s"DataFrame successfully written to Parquet at $outputPath")
  }
}

object RvPotentialConditionsGapUpdaterJob {
  def main(args: Array[String]): Unit = {
    raw_s3_path = "s3://testrawbucket123/raw/test.txt"
    val updater = new RvPotentialConditionsGapUpdater()
    
    updater.ReadPipeDelimitedFileFromS3(raw_s3_path)
  }
}
