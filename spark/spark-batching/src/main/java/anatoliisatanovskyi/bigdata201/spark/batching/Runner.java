package anatoliisatanovskyi.bigdata201.spark.batching;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import anatoliisatanovskyi.bigdata201.spark.batching.dataset.DatasetProvider;
import anatoliisatanovskyi.bigdata201.spark.batching.dataset.DatasetProviderService;
import anatoliisatanovskyi.bigdata201.spark.batching.service.HotelService;

public class Runner {

	protected static final Logger logger = LoggerFactory.getLogger(Runner.class);

	private Config config;
	private DatasetProvider datasetProvider = new DatasetProviderService();
	private HotelService hotelService = new HotelService();

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

		Dataset<Row> hotels = datasetProvider.getKafkaHotels(spark);
		Dataset<Row> expedia = datasetProvider.getAvroExpedia(spark);
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);

		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);
		logger.info("Invalid(idle) hotels:");
		idleHotels.show();

		Dataset<Row> validHotels = hotelService.getValidHotels(hotels, idleHotels);
		Dataset<Row> expediaWithValidHotels = hotelService.getExpediaWithValidHotels(expedia, validHotels);

		Dataset<Row> groupByCountryCount = hotelService.groupByColumnCount(expediaWithValidHotels, "Country");
		logger.info("Bookings by Country:");
		groupByCountryCount.show();

		Dataset<Row> groupByCityCount = hotelService.groupByColumnCount(expediaWithValidHotels, "City");
		logger.info("Bookings by City:");
		groupByCityCount.show();
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
