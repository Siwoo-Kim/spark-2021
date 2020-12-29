import org.apache.spark.sql.types._

object Schemas {

    val retailSchema = StructType(Array(
        StructField("InvoiceNo", LongType),
        StructField("StockCode", StringType),
        StructField("Description", StringType),
        StructField("Quantity", IntegerType),
        StructField("InvoiceDate", DateType),
        StructField("UnitPrice", DoubleType),
        StructField("CustomerID", DoubleType),
        StructField("Country", StringType)))
}
