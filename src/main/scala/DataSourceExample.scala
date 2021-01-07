import org.apache.spark.sql.functions.{current_date, split, trunc, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

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
 *  DataFrameReader (input)
 *      input 소스 객체.
 *      
 *      components.
 *          1. format - input 의 종류 지정
 *          2. option - 읽기시의 옵션 설정.
 *          3. schema - DataFrame 스키마
 *      
 *  DataFrameWriter (output)
 *      output 소스 객체.
 *      
 *      components.
 *          1. format - output 의 종류 지정.
 *          2. option - 쓰기시의 옵션 설정.
 *          3. partition - 레코드의 물리적 장소 지정. (layout of files)
 *          
 *  Parquet 파일.
 *      컬럼 단위의 저장 포맷의 파일
 *          ex)
 *              row1.col1, row2.col1, row3.col1, row1.col2, row2.col2, row3.col2 ...
 *              
 *      컬럼 단위의 저장의 장점. 
 *          1. 동일한 데이터가 모여있어 압축이 용이.
 *          2. 특정 컬럼만을 읽을시 io 비용이 감소.
 *          3. complex type 저장 가능.
 *  
 *  I/O 고급 개념.
 *  
 *      Layout of files
 *          병렬로 파일 읽기을 위한 파일 구조.
 *          다수의 노드가 같은 파일을 읽을 순 없기에
 *          데이터를 파티션으로 나눠 병렬 읽기를 처리.
 *          
 *          - 파일의 갯수는 DataFrame 의 파티션 갯수에 결정된다.
 *          
 *          1. partitioning.
 *              - 특정 컬럼을 기준으로 그룹화하여 디렉토리에 저장.
 *              - 읽기 처리시 원하는 데이터만 읽을 수 있어 io & 처리 비용 감소
 *          2. buckecting
 *      
 *     Complex type.
 *          - csv 파일은 Complex type 지원하지 않음ㅎ.
 *      
 *      Number of files.
 *          - 크기가 작은 파일이 많은 경우 io 연산 비용이 비쌈.
 *          - 크기가 너무 큰 파일인 경우 병렬성 저하.
 *          
 *          df.write.option("maxRecordsPerFile", 5000) 
 *              - 해당 옵션으로 파일당 최대 파일의 갯수를 조절 가능.
 *      
 */
object DataSourceExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("datasource")
                .getOrCreate()
        import spark.implicits._
        
        /**
         * READ MODE
         *  permissive  - corrupted record 에 대해서 null 로 설정. 이후에 _corrupt_record 에 저장.
         *      스키마에 _corrupted_record 컬럼이 존재하여야 함. (default)
         *  dropMalformed - corrupted record 을 drop.
         *  failfast - corrupted record 발견시 예외 발생.
         */
        spark.read.option("mode", "PERMISSIVE")
                .option("dataFormat", "yyyy-MM-dd hh:mm:ss")
                .option("header", true)
                .schema(Schemas.retailSchema)
                .csv(s"./data/*corrupted.csv")
                .show()
        
        spark.read.option("mode", "dropMalformed")
                .option("dateFormat", "yyyy-MM-dd hh:mm:ss")
                .option("header", true)
                .schema(Schemas.retailSchema)
                .csv(s"./data/*corrupted.csv")
                .show()

        /**
         * SAVE MODE (중요)
         *  append  - 파일이 존재한다면 끝에 추가.
         *  overwrite   - 파일이 존재한다면 덮어씀.
         *  errorIfExists   - 파일이 존재한다면 예외 던짐. (default)
         *  ignore  - 파일이 존재한다면 무시.
         */
        spark.range(10).toDF("num")
                .write
                .format("csv")
                .mode(SaveMode.Append)
                .save(s"./data/write-test1")

        /**
         * csv
         *  option components.
         *      1. 구분자 = sep
         *      2. 헤더 = header
         *      3. 스키마 자동 추론 = inferSchema
         *      4. 널 값 = nullValue
         *      5. 날짜 포맷 = dateFormat
         *      6. 다중 라인 (한 로우가 한행 이상 사용) = multiline
         *      
         */
        spark.read.format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .option("mode", "failfast")
                .load(s"${SparkData.PATH}/flight-data/csv/2010-summary.csv")
                .withColumn("time", current_date())
                .write
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", true)
                .option("dateFormat", "yyyy-MM-dd")
                .option("mode", "failfast")
                .save(s"./data/write-test2")

        /**
         * json
         *  option components.
         *      1. multiline = 전체 파일을 json object 로 인식. (파일 전체의 개행 여부)
         *      2. dateFormat
         */
        
        val jsonSchema = StructType(Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true),
            StructField("age", IntegerType, true)))
        spark.read.format("json")
                .option("mode", "failfast")
                .option("multiline", true)
                .schema(jsonSchema)
                .load(s"./data/json2.json")
                .show()

        println(spark.conf.get("spark.sql.parquet.mergeSchema"))
        
        /**
         * parquet files.
         *  option components.
         *      1. mergeSchema.
         *          "parquet 을 읽을시" 복수의 파켓에 
         *          여러 다른 schema 에 존재할 시 병합 여부 설정.
         *          
         *          spark.sql.parquet.mergeSchema - global 설정.
         *          
         *      2. compression.
         *          uncompressed, bzip2, gzip, snappy
         */
        spark.range(10).map(i => (i, i * i)).toDF("num", "square")
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .save(s"./data/square/key=1")
        
        spark.range(10).map(i => (i, i * i, i * i * i)).toDF("num", "square", "cube")
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .option("mergeSchema", true)
                .save(s"./data/square/key=2")
        
        spark.read.option("mergeSchema", true)
                .parquet(s"./data/square").show()

        /**
         * spark I/O concept.
         *  
         *  1. number of files (partitions)
         *  
         *  partitionBy(col) - 주어진 컬럼으로 그룹화하여 저장 파일을 디렉토리 레벨 (partition) 로 분배.
         *      장점. 읽기시 원하는 데이터만을 처리할 수 있어 속도 향상.
         *  bucketBy(bucketsize, col) - 주어진 컬럼으로 그룹화하여 파일 레벨 (bucket) 에서 분리하여 파일로 저장.
         *      - saveAsTable 로 저장해야 
         */
        spark.range(100)
                .repartition(10)
                .write
                .format("csv")
                .mode(SaveMode.Overwrite)
                .save(s"./data/partitions")
        
        spark.range(100).toDF("num")
                .withColumn("even", $"num" % 2 === 0)
                .write
                .partitionBy("even")
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save(s"./data/partitionBy")
    }
}
