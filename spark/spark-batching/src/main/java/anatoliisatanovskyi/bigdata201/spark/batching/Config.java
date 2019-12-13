package anatoliisatanovskyi.bigdata201.spark.batching;

import java.util.Properties;

public class Config {

	private static volatile Config INSTANCE;

	private final String defaultFS;
	private final String hdfsDir;
	private final String kafkaStreamReader;
	private final String kafkaTopic;
	private final Integer kafkaBatchSize;

	private Config(String defaultFS, String hdfsDir, String kafkaStreamReader, String kafkaTopic,
			Integer kafkaBatchSize) {
		this.defaultFS = defaultFS;
		this.hdfsDir = hdfsDir;
		this.kafkaStreamReader = kafkaStreamReader;
		this.kafkaTopic = kafkaTopic;
		this.kafkaBatchSize = kafkaBatchSize;
	}

	public String getDefaultFS() {
		return defaultFS;
	}

	public String getHdfsDir() {
		return hdfsDir;
	}

	public String getKafkaStreamReader() {
		return kafkaStreamReader;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public Integer getKafkaBatchSize() {
		return kafkaBatchSize;
	}

	public static Config load(Properties properties) {

		if (INSTANCE != null) {
			throw new IllegalStateException("configuration already loaded");
		}

		String defaultFS = properties.getProperty("defaultFS");
		String hdfsDir = properties.getProperty("hdfsDir");
		String kafkaStreamReader = properties.getProperty("kafkaStreamReader");
		String kafkaTopic = properties.getProperty("kafkaTopic");
		Integer kafkaBatchSize = Integer.parseInt(properties.getProperty("kafkaBatchSize"));
		INSTANCE = new Config(defaultFS, hdfsDir, kafkaStreamReader, kafkaTopic, kafkaBatchSize);
		System.out.println("config:" + INSTANCE);
		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}
}
