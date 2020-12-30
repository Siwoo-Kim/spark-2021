import org.apache.spark.sql.functions.{avg, count, countDistinct, lit, sum}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("dataframe")
                .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", 5)
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        
        /**
         * Create dataframe
         *  1. from raw source to Dataframe
         *  2. Rows or RDD to Dataframe
         *  
         *  JSON 은 데이터 로드시, 대소문자 구별
         */
        var flights = spark.read.schema(Schemas.flightSchema)
                .json(s"${SparkData.PATH}/flight-data/json/*.json")
        flights.createOrReplaceTempView("flights")
        
        val manualSchema = StructType(Array(
            StructField("some", StringType),
            StructField("col", StringType),
            StructField("names", LongType)))
        
        var rows = Seq(
            Row("hello", null, 1L),
            Row("spark", null, 2L))
        
        var rdd = spark.sparkContext.parallelize(rows)
        var df = spark.createDataFrame(rdd, manualSchema)
        df.show()

        /**
         * query on dataframe
         */
        flights = flights.select(
            $"dest_country_name".as("destination"), 
            $"origin_country_name".as("origin"), 
            $"count")
                .withColumn("within", $"destination" === $"origin")
        
        flights.where($"within").show()

        /**
         * simple agg
         */
        var agg = flights.select(avg($"count"), countDistinct($"destination"))
        agg.show()

        
        /**
         * literal
         */
        flights.select($"*", lit(1).as("one")).show(2)

        /**
         * adding & renaming column
         */
        flights.withColumn("one", lit(1)).show()
        flights.withColumnRenamed("within", "withinCountry").show()

        /**
         * drop columns *
         */
        flights.drop("within", "count").show()
        for (c <- flights.drop("within", "count").schema)
            println(c)

        /**
         * casting columns
         */
        flights.select($"count".cast("Long")).show(2)

        /**
         * filtering (and)
         */
        flights.where($"count" < 2)
                .where($"origin" =!= "Croatia")
                .show(2)
        
        flights.where(($"count"<2).or($"origin" =!= "Croatia"))
                .show(2)

        /**
         * distinct rows
         */
        flights.select("origin", "destination").distinct().show(2)

        /**
         * union 
         * 유니온 연산. 
         *  수직 결합. (열의 길이가 늘어난다.)
         *  유니온 연산은 위치 기반으로 수행된다.
         *  
         * 조인 연산.
         *  수평 결합. (행의 길이가 늘어난다.) 
         *  조인 연산은 외래키 기반으로 수행된다.
         */
        val schema = flights.schema
        rows = Seq(Row("Canada", "Korea", 101L, false), Row("Korea", "Japan", 202l, false))
        rdd = spark.sparkContext.parallelize(rows)
        var subFlights = spark.createDataFrame(rdd, schema)
        flights = flights.union(subFlights)
                .where(($"origin" === "Korea").or($"destination" === "Korea"))

        /**
         * 1. sorting (handling null)
         * 2. sorting for optimization (다른 transformation 수행 전 성능 개선을 위해 정렬)
         */
        flights.orderBy($"count".desc_nulls_last, $"destination".desc_nulls_first)
                .show(5)
        
        flights.sortWithinPartitions($"destination", $"count")
                .groupBy($"destination")
                .agg(sum($"count"))
                .show()

        /**
         * repartition and coalesce
         * 
         * repartition = control the physical layout of data across cluster. (shuffle) (병렬성 증가, 네트웍 트래픽 성능 감소)
         * coalesce = combine partitions (병렬성 감소, 네트웍 트래픽 성능 증가)
         */
        flights.repartition(5,$"destination")
                .where($"destination" === "Korea")
                .coalesce(1)
                .show()
    }
}
