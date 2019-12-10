package anatoliisatanovskyi.bigdata201.spark.batching;

import java.util.Properties;

public class Config {

	private static volatile Config INSTANCE;

	private final String sparkSqlWarehouseDir;
	private final Integer minIdleDays;
	private final Integer maxIdleDays;
	private final KafkaConfig kafkaConfig;

	private Config(String sparkSqlWarehouseDir, Integer minIdleDays, Integer maxIdleDays, KafkaConfig kafkaConfig) {
		this.sparkSqlWarehouseDir = sparkSqlWarehouseDir;
		this.minIdleDays = minIdleDays;
		this.maxIdleDays = maxIdleDays;
		this.kafkaConfig = kafkaConfig;
	}

	public String getSparkSqlWarehouseDir() {
		return sparkSqlWarehouseDir;
	}

	public Integer getMinIdleDays() {
		return minIdleDays;
	}

	public Integer getMaxIdleDays() {
		return maxIdleDays;
	}

	public KafkaConfig kafka() {
		return kafkaConfig;
	}

	public static Config load(Properties properties) {

		if (INSTANCE != null) {
			throw new IllegalStateException("configuration already loaded");
		}

		String sparkSqlWarehouseDir = properties.getProperty("sparkSqlWarehouseDir");
		Integer minIdleDays = Integer.parseInt(properties.getProperty("minIdleDays"));
		Integer maxIdleDays = Integer.parseInt(properties.getProperty("maxIdleDays"));
		String kafkaHostname = properties.getProperty("kafkaHostname");
		Integer kafkaPort = Integer.parseInt(properties.getProperty("kafkaPort"));
		String kafkaTopicHotelWeather = properties.getProperty("kafkaTopicHotelWeather");
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaHostname, kafkaPort, kafkaTopicHotelWeather);

		INSTANCE = new Config(sparkSqlWarehouseDir, minIdleDays, maxIdleDays, kafkaConfig);
		System.out.println("config:" + INSTANCE);
		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}

	static class KafkaConfig {
		private final String hostname;
		private final Integer port;
		private final String topicHotelWeather;

		public KafkaConfig(String hostname, Integer port, String topicHotelWeather) {
			this.hostname = hostname;
			this.port = port;
			this.topicHotelWeather = topicHotelWeather;
		}

		public String getHostname() {
			return hostname;
		}

		public Integer getPort() {
			return port;
		}

		public String getTopicHotelWeather() {
			return topicHotelWeather;
		}
	}
}
