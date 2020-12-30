import org.apache.spark.sql.SparkSession

object SchemaExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("schema")
                .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", 5)
        
        //forcing schema
        val flights = spark.read
                .schema(Schemas.flightSchema)
                .json(s"${SparkData.PATH}/flight-data/json/*.json")
        flights.printSchema()
        
        val schema = flights.schema
        println(schema)
    }
}
