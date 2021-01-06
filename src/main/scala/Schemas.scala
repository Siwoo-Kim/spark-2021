import org.apache.spark.sql.types._

object Schemas {
    
    val flightSchema = StructType(Array(
        StructField("DEST_COUNTRY_NAME", StringType),
        StructField("ORIGIN_COUNTRY_NAME", StringType),
        StructField("count", LongType)))

    val retailSchema = StructType(Array(
        StructField("InvoiceNo", StringType),
        StructField("StockCode", StringType),
        StructField("Description", StringType),
        StructField("Quantity", IntegerType),
        StructField("InvoiceDate", DateType),
        StructField("UnitPrice", DoubleType),
        StructField("CustomerID", DoubleType),
        StructField("Country", StringType),
        StructField("_corrupt_record", StringType)
    ))
}
