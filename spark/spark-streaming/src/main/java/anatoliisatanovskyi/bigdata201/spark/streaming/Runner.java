package anatoliisatanovskyi.bigdata201.spark.streaming;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Runner {

	private Config config;

	public Runner(Config config) {
		this.config = config;
	}

	public static void main(String[] args) {

		try {
			Config config = loadConfiguration(args);
			Runner runner = new Runner(config);
			runner.execute();
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			System.out.println("exiting...");
			System.exit(0);
		}
	}

	public void execute() throws AnalysisException {

		SparkSession spark = createSparkSession();
		spark.sql("CREATE TEMPORARY VIEW expedia2 USING com.databricks.spark.avro OPTIONS (path '/expedia')");

		// Read Expedia data for 2016 year from HDFS as initial state DataFrame
		Dataset<Row> expedia2016 = spark.sql("SELECT * FROM expedia2 WHERE year(srch_ci) = 2016").alias("expedia2016");

		// join with hotels+weather data from Kafka topic
		/* Dataset<Row> kafka = */spark.read().format("org.apache.spark.sql.kafka010.KafkaSourceProvider")//
				.option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")//
				.option("subscribe", "kafka_weather3")//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("failOnDataLoss", "false") //
				.option("maxOffsetsPerTrigger", 200)//
				.load()
				.select(from_json(col("value").cast(DataTypes.StringType), kafkaTopicStructSchema()).alias("tmp"))//
				.select("tmp.*")//
				.alias("kafka");

		// Dataset<Row> kafka1 = kafka.select("kafka.*").alias("kafka1");

		// add average day temperature at checkin
		// Filter incoming data by having average temperature more than 0 Celsius
		// degrees
		Dataset<Row> hotelWeather = spark
				.sql("SELECT hotelId,wthr_date,avg_tmpr_c FROM kafka_hotel_weather WHERE avg_tmpr_c > 0");

		Dataset<Row> enriched2016 = expedia2016
				.join(hotelWeather, expedia2016.col("hotel_id").equalTo(hotelWeather.col("hotelId")), "left_inner")
				.alias("enriched2016");

		enriched2016.show(10);

		// Calculate customer's duration of stay as days between requested check-in and
		// check-out date

		//
	}

	private SparkSession createSparkSession() {
		SparkConf sparkConf = createSparkConfig();
		return SparkSession.builder().config(sparkConf).getOrCreate();
	}

	private SparkConf createSparkConfig() {
		return new SparkConf().setAppName("Java Spark Hive ETL")//
				.set("fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020")//
				.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")//
				.set("spark.master", "local");
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

	private static Config loadConfiguration(String[] args) throws IOException {

		if (args.length == 0) {
			throw new IllegalArgumentException("properties filepath argument is required");
		}

		String propertiesFilePath = args[0];
		Properties properties = new Properties();
		try (FileInputStream fis = new FileInputStream(new File(propertiesFilePath))) {
			properties.load(fis);
			return Config.load(properties);
		}
	}
}
