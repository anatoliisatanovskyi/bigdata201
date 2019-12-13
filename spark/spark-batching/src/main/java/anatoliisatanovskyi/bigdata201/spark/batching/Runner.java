package anatoliisatanovskyi.bigdata201.spark.batching;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import anatoliisatanovskyi.bigdata201.spark.batching.json.JsonMapper;
import anatoliisatanovskyi.bigdata201.spark.batching.model.HotelStay;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.year;
import static org.apache.spark.sql.functions.count;

public class Runner {

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	private Config config = Config.instance();
	private JsonMapper jsonMapper = new JsonMapper();

	public static void main(String[] args) {

		try {

			debugLog("loading configuration...");
			loadConfiguration(args);
			debugLog("configuration loaded");

			Runner runner = new Runner();

			debugLog("calling execute method...");
			runner.execute();
			debugLog("execute finished");

		} catch (Throwable e) {
			logger.error("an error occured: " + e.getMessage(), e);
		} finally {

			System.exit(0);
		}
	}

	// {"Id":"893353197568","Name":"Quality Inn &
	// Suites","Country":"US","City":"Denver","Address":"12085 Delaware
	// St","Latitude":"39.915086","Longitude":"-104.992635","geohash":"9xj74"}

	public void execute() throws StreamingQueryException, AnalysisException {

		StructType schema = new StructType();
		schema.add("Id", DataTypes.StringType);
		schema.add("Name", DataTypes.StringType);
		schema.add("Country", DataTypes.StringType);
		schema.add("City", DataTypes.StringType);
		schema.add("Address", DataTypes.StringType);
		schema.add("Latitude", DataTypes.StringType);
		schema.add("Longitude", DataTypes.StringType);
		schema.add("geohash", DataTypes.StringType);

		SparkSession spark = createSparkSession();
		Dataset<Row> kafka = spark.read().format(config.getKafkaStreamReader())
				.option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
				.option("subscribe", config.getKafkaTopic())//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("startingOffsets", "earliest")//
				.load();

		kafka = kafka.select(from_json(kafka.col("value").cast("string"), schema)).alias("kafka");
		Dataset<Row> kafka1 = kafka.select("kafka.*").alias("kafka1");

		kafka.createTempView("table");

		spark.sql("CREATE TEMPORARY TABLE expedia2 USING com.databricks.spark.avro OPTIONS (path '/expedia')");
		Dataset<Row> expedia_data = spark.sql("SELECT *,year(srch_ci) as year_srch_ci FROM expedia2")
				.alias("expedia_data");

		Dataset<Row> idle_data = spark.sql("SELECT \r\n" + "				    s1.hotel AS hotel_invalid_id,\r\n"
				+ "				    s1.s_date, \r\n" + "				    s1.e_date, \r\n"
				+ "				    s1.l_date, \r\n" + "				    s1.idle, \r\n"
				+ "				    table.kafka.Name, \r\n" + "				    table.kafka.Country, \r\n"
				+ "				    table.kafka.City, \r\n" + "				    table.kafka.Address\r\n"
				+ "				FROM\r\n" + "				    (SELECT \r\n"
				+ "				        s.hotel_id AS hotel,\r\n" + "				        s.start_date AS s_date,\r\n"
				+ "					    s.end_date AS e_date,\r\n"
				+ "					    s.lead_start_date AS l_date,\r\n"
				+ "					    datediff(s.lead_start_date, s.end_date) AS idle\r\n"
				+ "				    FROM \r\n" + "					    (SELECT \r\n"
				+ "					        hotel_id,\r\n" + "					 	    srch_ci AS start_date,\r\n"
				+ "					 		srch_co AS end_date,\r\n"
				+ "					 		lead(srch_ci,1,0) OVER (PARTITION BY hotel_id ORDER BY srch_ci ASC) AS lead_start_date  \r\n"
				+ "						FROM expedia2\r\n"
				+ "						GROUP BY hotel_id, srch_ci, srch_co\r\n" + "					    ) s\r\n"
				+ "					) s1\r\n" + "				JOIN TABLE ON (hotel=table.kafka.Id)\r\n"
				+ "				WHERE s1.idle>=2 AND s1.idle<30\r\n" + "				GROUP BY \r\n"
				+ "				    hotel_invalid_id,\r\n" + "				    s1.s_date, \r\n"
				+ "				    s1.e_date, \r\n" + "				    s1.l_date, \r\n"
				+ "				    s1.idle, \r\n" + "				    table.kafka.Name,\r\n"
				+ "				    table.kafka.Country, \r\n" + "				    table.kafka.City, \r\n"
				+ "				    table.kafka.Address").alias("idle_data");

		Dataset<Row> correct_data = expedia_data //
				.join(idle_data, expedia_data.col("hotel_id").equalTo(idle_data.col("hotel_invalid_id")), "left_outer") //
				.select("expedia_data.*").alias("correct_data");

		correct_data.write().partitionBy("year_srch_ci").csv("/spark_expedia/");

		Dataset<Row> correct_data_extended = correct_data //
				.join(kafka1, correct_data.col("hotel_id").equalTo(kafka1.col("Id"))) //
				.select("correct_data.hotel_id", "kafka1.Country", "kafka1.City") //
				.groupBy(kafka1.col("Country"), kafka1.col("City")) //
				.agg(count("correct_data.hotel_id")) //
				.alias("correct_data_extended");

		correct_data_extended.show(30);
	}

	public Dataset<Row> readHdfsDataset(SparkSession sparkSession) {
		return sparkSession.read().format("com.databricks.spark.avro").load(config.getHdfsDir());
	}

	public Dataset<Row> readKafkaDataset(SparkSession sparkSession) throws StreamingQueryException {
		return sparkSession.read().format(config.getKafkaStreamReader())
				.option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
				.option("subscribe", config.getKafkaTopic())//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("startingOffsets", "earliest")//
				/* .option("maxOffsetsPerTrigger", "100") */.load();
	}

	private SparkSession createSparkSession() {
		SparkConf sparkConf = createSparkConfig();
		return SparkSession.builder().config(sparkConf).getOrCreate();
	}

	private SparkConf createSparkConfig() {
		return new SparkConf().setAppName("Java Spark Hive ETL")//
				.set("fs.defaultFS", config.getDefaultFS())//
				.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")//
				.set("spark.master", "local");
	}

	public static void loadConfiguration(String[] args) throws IOException {
		if (args.length == 0) {
			throw new IllegalArgumentException("configuration filepath is required");
		}
		String configFilepath = args[0];
		loadConfiguration(configFilepath);
	}

	public static void loadConfiguration(String configFilepath) throws IOException {
		try (FileInputStream fis = new FileInputStream(new File(configFilepath))) {
			Properties properties = new Properties();
			properties.load(fis);
			Config.load(properties);
		}
	}

	private static void debugLog(Object... args) {
		logger.info("TAG_DEBUG: " + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(" ")));
	}
}
