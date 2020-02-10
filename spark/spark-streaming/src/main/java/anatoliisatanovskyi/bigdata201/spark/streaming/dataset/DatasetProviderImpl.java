package anatoliisatanovskyi.bigdata201.spark.streaming.dataset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import anatoliisatanovskyi.bigdata201.spark.streaming.Config;

public class DatasetProviderImpl implements DatasetProvider {

	private Config config;

	private DatasetProviderImpl(Config config) {
		this.config = config;
	}

	public static DatasetProvider create(SparkSession spark, Config config) {
		spark.sql("CREATE TEMPORARY VIEW expedia2 USING com.databricks.spark.avro OPTIONS (path '" + config.getHdfsDir()
				+ "')");
		return new DatasetProviderImpl(config);
	}

	@Override
	public Dataset<Row> getHotelWeather(SparkSession spark) {
		return spark.read().format(config.getKafkaStreamReader())//
				.option("kafka.bootstrap.servers", config.getKafkaConsumerListener())//
				.option("subscribe", config.getKafkaTopic())//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("failOnDataLoss", "false") //
				.load()
				.select(from_json(col("value").cast(DataTypes.StringType), kafkaTopicStructSchema()).alias("tmp"))//
				.select("tmp.*")//
				.alias("kafka");
	}

	@Override
	public Dataset<Row> getExpedia(SparkSession spark, int year) {
		return spark
				.sql("SELECT hotel_id,srch_co,srch_ci,srch_children_cnt FROM expedia2 WHERE year(srch_ci) = " + year);
	}

	private StructType kafkaTopicStructSchema() {
		return new StructType()//
				.add("hotelId", DataTypes.StringType, true)//
				.add("name", DataTypes.StringType, true)//
				.add("country", DataTypes.StringType, true)//
				.add("city", DataTypes.StringType, true)//
				.add("address", DataTypes.StringType, true)//
				.add("avg_tmpr_f", DataTypes.DoubleType, true)//
				.add("avg_tmpr_c", DataTypes.DoubleType, true)//
				.add("wthr_date", DataTypes.StringType, true)//
				.add("geohashPrecision", DataTypes.IntegerType, true);
	}
}
