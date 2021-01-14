import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
 * Distributed Variables.
 *  분산 변수.
 *  
 *  broadcast variables.
 *      - 불변의 값으로 모든 노드간 공유.
 *      - 클로저의 변수는 직렬화 비용이 필요한 반면,
 *      broadcast variables 은 노드에 캐쉬므로 비용이 감소.
 *
 *  accumulators
 *      - 다양한 transformation 에서 공유 가변 변수 (shared mutable variable).
 *      - action 에서만 변수의 값 변경 가능.
 */
object DistributedVariablesExample {

    case class Flight(destination: String, origin: String, count: Long)
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("distributedVariables")
                .getOrCreate()
        
        val path = Paths.get("./src/main/resources/words.txt")
        val words = spark.sparkContext.textFile(s"${path.toUri}", 2)
                .flatMap(line => line.split("\\s+"))

        /**
         * broadcast variables.
         *  broadcasting
         *      val bv = spark.sparkContext.broadcast(val)
         *  
         *  accessing the value in broadcast variable
         *      bv.value
         */
        val scores = Map("spark" -> 1000, "definitive" -> 200, "big" -> -300, "simple" -> 100)
        val broadcasted = spark.sparkContext.broadcast(scores)
        
        words.map(w => (w, broadcasted.value.getOrElse(w, 0)))
                .sortBy(w => w._2)
                .foreach(w => println(w))

        /**
         * accumulator
         *  1. accumulator variables.
         *      long accumulator, double accumulator,
         *      collection accumulator (CollectionAccumulator)
         *      
         *      val acc = new LongAccumulator
         *      
         *  2. register the accumulator
         *      spark.sparkContext.register(acc)
         *   
         *  3. update & access the value
         *      acc.add
         *      acc.value
         */

        import spark.implicits._
        
        val flight = spark.read
                .parquet(s"${SparkData.PATH}/flight-data/parquet/2010-summary.parquet")
                .withColumnRenamed("dest_country_name", "destination")
                .withColumnRenamed("origin_country_name", "origin")
                .as[Flight]
        
        val acc = new LongAccumulator
        spark.sparkContext.register(acc, "china")
        
        def accChina(flight: Flight, country: String): Unit = {
            if (country == flight.destination)
                acc.add(flight.count)
            if (country == flight.origin)
                acc.add(flight.count)
        }
        flight.foreach(f => accChina(f, "China"))
        
        println(acc.value)
    }
}
