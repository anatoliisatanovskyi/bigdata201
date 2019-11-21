package anatoliisatanovskyi.bigdata201.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Runner {

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	public static void main(String[] args) {

		try {
			loadConfiguration(args);
			HotelsWeatherEnricher hotelsWeatherProcessor = new HotelsWeatherEnricher();
			try {
				hotelsWeatherProcessor.init();
				hotelsWeatherProcessor.prepareHotelsData();
				hotelsWeatherProcessor.consumeWeatherProduceEnriched();
				hotelsWeatherProcessor.awaitTillFinished();
			} finally {
				hotelsWeatherProcessor.terminate();
			}
		} catch (Exception e) {
			logger.error("an error occured: " + e.getMessage(), e);
		} finally {

			System.exit(0);
		}
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
}
