import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, sum, when, window}

object SparkStreaming {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("streaming")
                .getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        import spark.implicits._
        spark.conf.set("spark.sql.shuffle.partitions", 5)
        
        val stream = spark.readStream
                .schema(Schemas.retailSchema)
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .option("mode", "failfast")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
                .option("maxFileTriggers", 1)
                .load(s"${SparkData.PATH}/retail-data/by-day/*.csv")
        println(stream.isStreaming)
        
        val summary = stream.select($"customerId", expr("unitprice * quantity").as("cost"), $"invoicedate")
                .groupBy($"customerId", window($"invoicedate", "1 day"))
                .agg(sum($"cost").as("dayCost"))
        
        val query = summary.writeStream.format("memory")
                .queryName("retail_summary")
                .outputMode("complete")
                .start()
        
        while (query.isActive) {
            spark.sql("select * from retail_summary").show()
        }
    }
}
