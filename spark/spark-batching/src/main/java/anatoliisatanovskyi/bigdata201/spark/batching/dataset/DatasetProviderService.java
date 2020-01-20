package anatoliisatanovskyi.bigdata201.spark.batching.dataset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import anatoliisatanovskyi.bigdata201.spark.batching.Config;

public class DatasetProviderService implements DatasetProvider {

	private Config config = Config.instance();

	@Override
	public Dataset<Row> getKafkaHotels(SparkSession spark) {
		return spark.read().format(config.getKafkaStreamReader())//
				.option("kafka.bootstrap.servers", config.getKafkaConsumerListener())//
				.option("subscribe", config.getKafkaTopic())//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("failOnDataLoss", "false") //
				.option("maxOffsetsPerTrigger", config.getKafkaBatchSize())//
				.load()
				.select(from_json(col("value").cast(DataTypes.StringType), kafkaHotelsStructSchema()).alias("tmp"))//
				.select("tmp.*")//
				.alias("kafka");
	}

	@Override
	public Dataset<Row> getAvroExpedia(SparkSession spark) {
		return spark.read().format("com.databricks.spark.avro").load(config.getDefaultFS() + config.getHdfsDir())
				.alias("expedia_data");
	}

	private StructType kafkaHotelsStructSchema() {
		return new StructType()//
				.add("Id", DataTypes.StringType, true)//
				.add("Name", DataTypes.StringType, true)//
				.add("Country", DataTypes.StringType, true)//
				.add("City", DataTypes.StringType, true)//
				.add("Address", DataTypes.StringType, true)//
				.add("Latitude", DataTypes.StringType, true)//
				.add("Longitude", DataTypes.StringType, true)//
				.add("geohash", DataTypes.StringType, true);
	}
}
