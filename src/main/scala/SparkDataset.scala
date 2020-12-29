import org.apache.spark.sql.SparkSession

object SparkDataset {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("dataset")
                .getOrCreate()
        import spark.implicits._

        /**
         * create dataset
         */
        val flights = spark.read.parquet(s"${SparkData.PATH}/flight-data/parquet/2010*.parquet/")
                .select($"dest_country_name".as("destination"), $"origin_country_name".as("origin"), $"count")
                .as[Flight]
                .filter(flight => flight.destination != "Canada")
                .take(5)
        for (f <- flights)
            println(f)
    }
    
}
