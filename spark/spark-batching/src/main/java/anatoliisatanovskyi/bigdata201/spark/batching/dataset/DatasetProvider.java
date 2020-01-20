package anatoliisatanovskyi.bigdata201.spark.batching.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DatasetProvider {

	Dataset<Row> getKafkaHotels(SparkSession spark);
	
	Dataset<Row> getAvroExpedia(SparkSession spark);
}
