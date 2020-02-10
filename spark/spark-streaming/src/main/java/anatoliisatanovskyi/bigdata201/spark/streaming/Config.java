package anatoliisatanovskyi.bigdata201.spark.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {

	private static volatile Config INSTANCE;

	private final String defaultFS;
	private final String hdfsDir;
	private final String kafkaConsumerListener;
	private final String kafkaStreamReader;
	private final String kafkaTopic;
	private final String outputPath;
	private final String outputType;

	private Config(String defaultFS, String hdfsDir, String kafkaConsumerListener, String kafkaStreamReader,
			String kafkaTopic, String outputPath, String outputType) {
		this.defaultFS = defaultFS;
		this.hdfsDir = hdfsDir;
		this.kafkaConsumerListener = kafkaConsumerListener;
		this.kafkaStreamReader = kafkaStreamReader;
		this.kafkaTopic = kafkaTopic;
		this.outputPath = outputPath;
		this.outputType = outputType;
	}

	public String getDefaultFS() {
		return defaultFS;
	}

	public String getHdfsDir() {
		return hdfsDir;
	}

	public String getKafkaConsumerListener() {
		return kafkaConsumerListener;
	}

	public String getKafkaStreamReader() {
		return kafkaStreamReader;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getOutputType() {
		return outputType;
	}

	public static Config load(String path) throws FileNotFoundException, IOException {
		try (FileInputStream fis = new FileInputStream(new File(path))) {
			Properties properties = new Properties();
			properties.load(fis);
			return load(properties);
		}
	}

	public static Config load(Properties properties) {

		if (INSTANCE != null) {
			throw new IllegalStateException("configuration already loaded");
		}

		String defaultFS = properties.getProperty("defaultFS");
		String hdfsDir = properties.getProperty("hdfsDir");
		String kafkaConsumerListener = properties.getProperty("kafkaConsumerListener");
		String kafkaStreamReader = properties.getProperty("kafkaStreamReader");
		String kafkaTopic = properties.getProperty("kafkaTopic");
		String outputPath = properties.getProperty("outputPath");
		String outputType = properties.getProperty("outputType");
		INSTANCE = new Config(defaultFS, hdfsDir, kafkaConsumerListener, kafkaStreamReader, kafkaTopic, outputPath,
				outputType);

		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}

}
