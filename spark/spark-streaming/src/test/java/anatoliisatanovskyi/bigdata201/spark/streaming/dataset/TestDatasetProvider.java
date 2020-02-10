package anatoliisatanovskyi.bigdata201.spark.streaming.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class TestDatasetProvider extends SharedJavaSparkContext implements DatasetProvider {

	@Override
	public Dataset<Row> getHotelWeather(SparkSession spark) {
		return spark.read().json(this.getClass().getClassLoader().getResource("dataset/hotelWeather.json").getFile());
	}

	@Override
	public Dataset<Row> getExpedia(SparkSession spark, int year) {
		return spark.read().format("avro")
				.load(this.getClass().getClassLoader().getResource("dataset/expedia/").getFile());
	}
}
