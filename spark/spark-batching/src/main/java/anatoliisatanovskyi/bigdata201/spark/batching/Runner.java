package anatoliisatanovskyi.bigdata201.spark.batching;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Runner {

	protected static final Logger logger = LoggerFactory.getLogger(Runner.class);

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
			logger.error("an error occured: " + e.getMessage(), e);
		} finally {

			System.exit(0);
		}
	}

	public void execute() throws StreamingQueryException, AnalysisException {

		SparkSession spark = createSparkSession();

		Dataset<Row> kafka = spark.read().format(config.getKafkaStreamReader())//
				.option("kafka.bootstrap.servers", config.getKafkaConsumerListener())//
				.option("subscribe", config.getKafkaTopic())//
				.option("startingOffsets", "earliest")//
				.option("endingOffsets", "latest")//
				.option("failOnDataLoss", "false") //
				.option("maxOffsetsPerTrigger", config.getKafkaBatchSize())//
				.load()
				.select(from_json(col("value").cast(DataTypes.StringType), kafkaTopicStructSchema()).alias("tmp"))//
				.select("tmp.*")//
				.alias("kafka");

		Dataset<Row> kafka1 = kafka.select("kafka.*").alias("kafka1");

		kafka.createTempView("table");

		spark.sql("CREATE TEMPORARY VIEW expedia2 USING com.databricks.spark.avro OPTIONS (path '/expedia')");

		Dataset<Row> expedia_data = spark.sql("SELECT *, year(srch_ci) as year_srch_ci FROM expedia2")
				.alias("expedia_data");

		Dataset<Row> idle_data = spark.sql("SELECT \r\n" + "				    s1.hotel AS hotel_invalid_id,\r\n"
				+ "				    s1.s_date, \r\n" + "				    s1.e_date, \r\n"
				+ "				    s1.l_date, \r\n" + "				    s1.idle, \r\n"
				+ "				    table.Name, \r\n" + "				    table.Country, \r\n"
				+ "				    table.City, \r\n" + "				    table.Address\r\n"
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
				+ "					) s1\r\n" + "				JOIN TABLE ON (hotel=table.Id)\r\n"
				+ "				WHERE s1.idle>=2 AND s1.idle<30\r\n" + "				GROUP BY \r\n"
				+ "				    hotel_invalid_id,\r\n" + "				    s1.s_date, \r\n"
				+ "				    s1.e_date, \r\n" + "				    s1.l_date, \r\n"
				+ "				    s1.idle, \r\n" + "				    table.Name,\r\n"
				+ "				    table.Country, \r\n" + "				    table.City, \r\n"
				+ "				    table.Address").alias("idle_data");

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

	private StructType kafkaTopicStructSchema() {
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

	public static Config loadConfiguration(String[] args) throws IOException {
		if (args.length == 0) {
			throw new IllegalArgumentException("configuration filepath is required");
		}
		String configFilepath = args[0];
		return Config.load(configFilepath);
	}
}
