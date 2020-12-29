import org.apache.spark.sql.SparkSession

object SparkSubmit {

    def main(args: Array[String]): Unit = {
        /**
         * $SPARK_HOME/bin/spark-submit --class SparkSubmit --master local ./target/spark-2021-0.0.1.jar [args...]
         */
        val spark = SparkSession.builder()
                .master("local")
                .appName("sparksubmit")
                .getOrCreate()
        import spark.implicits._
        
        spark.range(args(0).toInt).toDF("num")
                .where($"num" % 2 === 0)
                .show()
    }
}
