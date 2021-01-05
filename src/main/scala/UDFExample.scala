import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 * udf
 *  - 사용자 정의 transformation
 *  - 레코드 단위의 연산 함수.
 *  - SparkSession 혹은 Context 에 등록되어 사용.
 *  - 드라이버에서 직렬화되어 노드로 전달되어 실행.
 *  
 *  udf 등록.
 *      1. udf(func)
 *      2. spark.udf.register(함수명, func)
 */
object UDFExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("udf")
                .getOrCreate()
        import spark.implicits._
        
        def power3(n: Double): Double = n * n * n
        var power3UDF = udf(power3(_:Double):Double) 
        val df = spark.range(10).toDF("num")
                
        df.select($"num", power3UDF($"num").as("power3"))
                .show()
        
        df.createOrReplaceTempView("nums")
        spark.udf.register("power3", power3(_:Double):Double)
        spark.sql("select num, power3(num) as power3 from nums").show()
    }
}
