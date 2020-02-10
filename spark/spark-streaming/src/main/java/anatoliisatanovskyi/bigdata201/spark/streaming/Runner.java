package anatoliisatanovskyi.bigdata201.spark.streaming;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anatoliisatanovskyi.bigdata201.spark.streaming.dataset.DatasetProvider;
import anatoliisatanovskyi.bigdata201.spark.streaming.dataset.DatasetProviderImpl;
import anatoliisatanovskyi.bigdata201.spark.streaming.service.HotelService;
import anatoliisatanovskyi.bigdata201.spark.streaming.writer.OutputWriter;

public class Runner {

	protected static final Logger logger = Logger.getLogger(Runner.class);

	private Config config;

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
			logger.error("An error occured:" + e.getMessage(), e);
		} finally {
			System.exit(0);
		}
	}

	public void execute() throws AnalysisException {

		SparkSession spark = createSparkSession();
		DatasetProvider datasetProvider = DatasetProviderImpl.create(spark, config);

		Dataset<Row> expedia2016 = broadcast(datasetProvider.getExpedia(spark, 2016));
		Dataset<Row> expedia2017 = datasetProvider.getExpedia(spark, 2017);
		Dataset<Row> hotelWeather = datasetProvider.getHotelWeather(spark).groupBy(col("hotelId"), col("wthr_date"))
				.agg(avg(col("avg_tmpr_c")).as("avg_tmpr_c"));
		Dataset<Row> expedia = expedia2016.union(expedia2017);

		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		ds = hotelService.enrichWithStayDuration(ds);
		ds = hotelService.enrichWithChildrenPresent(ds);

		Dataset<Row> customerPreference = hotelService.calculateCustomerPreference(ds);
		customerPreference = hotelService.enrichWithRandomizedTimestamp(customerPreference);

		OutputWriter outputWriter = OutputWriter.instanceOf(config.getOutputType());
		outputWriter.write(customerPreference, config.getOutputPath());
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
