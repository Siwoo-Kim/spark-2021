import org.apache.spark.sql.SparkSession

object SparkRDD {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("rdd")
                .getOrCreate()
        
        val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3))
        rdd.foreach(e => println(e))
    }
}
