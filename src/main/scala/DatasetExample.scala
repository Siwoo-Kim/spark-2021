import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

/**
 *  Dataset
 *      - jvm 에 정의된 타입이 정해진 record. (compile-time type-safety)
 *      - 스칼라에선 case class 로 schema 정의.
 *      - Dataframe 은 row type 의 dataset.
 *      - Encoder 을 통한 jvm class 을 스파크 내부 타입으로 인코딩.
 *  
 *  왜 Dataset 인가?
 *      - Dataframe 으로 연산을 표현하지 못할 때.
 *      - 타입 안전하게 사용하고 싶을 때.
 *  
 *   Dataset 의 api 은 Dataframe 이 "모두 사용 가능"하다.
 *      Dataframe 은 Dataset 이므
 */
object DatasetExample {

    /**
     * case class.
     *  1. 불변.
     *  2. 패턴 매칭에서 분할 가능. (부모 클래스 to case class)
     *  3. 레퍼런스가 아닌 값으로 비교 가능. (as if there were primitive values)
     */
    case class Flight(destination: String, origin: String, count: Long)

    case class Meta(count: Long, randomData: Long)
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("dataset")
                .getOrCreate()
        import spark.implicits._
        
        
        val df = spark.read.parquet(s"${SparkData.PATH}/flight-data/parquet/*.parquet")
                .withColumnRenamed("dest_country_name", "destination")
                .withColumnRenamed("origin_country_name", "origin")
        /**로
         * Dataframe to Dataset
         *  as 메서드와 Encoder 지정.
         *  
         *  as[Encoder]
         */
        val flights = df.as[Flight]

        /**
         * actions 
         *  - execute the transformations plan
         *  collect
         *  take
         *  count
         */
        flights.show(2)
        val dest = flights.first().destination
        println(dest)

        /**
         * 함수 전달.
         *  함수 전달시, 함수는 각 노드에 복사되어 수행되어야 하므로 비용이 증가할 수 있음.
         *  함수를 꼭 사용할 이유가 없다면 sql expression 을 사용.
         *  
         */
        //filter
        flights.filter(f => f.origin == f.destination).show()
        
        //mapping
        flights.map(f => f.destination).show()

        /**
         * joinWith
         *      두 개의 dataset 을 하나로 합침. (two nested dataset inside of one)
         *      ds1.joinWith(ds2, joinExp)
         *      
         *          _1      _2
         *          ds1     ds2
         */
        val meta = spark.range(500)
                .map(x => (x, scala.util.Random.nextLong))
                .withColumnRenamed("_1", "count")
                .withColumnRenamed("_2", "randomData")
                .as[Meta]
        val joined = flights.joinWith(meta, flights.col("count") === meta.col("count"))
        
        joined.show()
        joined.select("_1.destination").show()

        /**
         * 그룹핑 & 집계
         * 
         *  dataset.groupBy(col)
         *          - 그룹화한 "DataFrame" 을 리턴.
         *  dataset.groupByKey(data => data.key)
         *          - 그룹화한 "Dataset" 을 리턴.
         *  
         *      flatMapGroups
         *          => 키에 관련된 그룹의 데이터에 관한 연산을 적용. (key: String, valuesForKey: Iterator[U])
         *          
         *      mapValues
         *          => 값에 대한 transformation.
         *          map => (key & value)
         *          mapValues => value
         *      
         *      
         */
        
        flights.groupBy("destination")
                .agg(count(expr("*")))
                .show()
        
        case class NewFlight()
        def sum(key: String, values: Iterator[Flight]) = {
            values.dropWhile(_.count < 5).map(f => (key, f))
        }
        
        flights.groupByKey(flight => flight.destination)
                .flatMapGroups(sum)
                .show()
        
        flights.groupByKey(f => f.destination)
                .mapValues(f => 1)
                .count()
                .show()
        
    }
}
