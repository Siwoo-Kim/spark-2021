import org.apache.spark.sql.SparkSession

/**
 * DataSource
 *  input sources.
 *  
 *  1. csv
 *  2. json
 *  3. parquet (delta)
 *  4. orc
 *  5. jdbc connections
 *  6. plain-text files
 *  7. avro
 *  
 *  DataFrameReader
 *      input 소스 객체.
 *      
 *      components.
 *          1. format - input 의 종류 지정
 *          2. option - 읽기시의 옵션 설정.
 *          3. schema - DataFrame 스키마
 *      
 * 
 */
object DataSourceExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("datasourc")
                .getOrCreate();
        
        /**
         * READ MODE
         *  permissive  - corrupted record 에 대해서 null 로 설정. 이후에 _corrupt_record 에 저장.
         *  dropMalformed - co
         */
        val df = spark.read.option("mode", "PERMISSIVE")
                .option("dataFormat", "yyyy-MM-dd hh:mm:ss")
                .option("header", true)
                .schema(Schemas.retailSchema)
                .csv(s"./data/*corrupted.csv")
        df.show()
        
    }
}
