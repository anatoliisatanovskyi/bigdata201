package anatoliisatanovskyi.bigdata201.spark.streaming.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DatasetProvider {

	Dataset<Row> getHotelWeather(SparkSession spark);

	Dataset<Row> getExpedia(SparkSession spark, int year);
	
}
