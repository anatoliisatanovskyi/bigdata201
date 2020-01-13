package anatoliisatanovskyi.bigdata201.spark.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Runner {

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	private Config config;

	public Runner(Config config) {
		this.config = config;
	}

	public static void main(String[] args) {

		try {
			Config config = loadConfiguration(args);
			Runner runner = new Runner(config);
			runner.execute();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			System.exit(0);
		}
	}

	public void execute() {
		
		// Read Expedia data for 2016 year from HDFS as initial state DataFrame
		
		// Read data for 2017 year as streaming data
		
		// Join streams ?
		
		// join with hotels+weaher data from Kafka topic
		
		// add average day temperature at checkin
		
		// Filter incoming data by having average temperature more than 0 Celsius degrees
		
		// Calculate customer's duration of stay as days between requested check-in and check-out date
		
		// 
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
