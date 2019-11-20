package anatoliisatanovskyi.bigdata201.kafka;

import java.util.Properties;

public class Config {

	private static volatile Config INSTANCE;

	private final GeneralConfig generalConfig;
	private final KafkaConfig kafkaConfig;
	private final HiveConfig hiveConfig;

	private Config(GeneralConfig generalConfig, KafkaConfig kafkaConfig, HiveConfig hiveConfig) {
		this.generalConfig = generalConfig;
		this.kafkaConfig = kafkaConfig;
		this.hiveConfig = hiveConfig;
	}

	@Override
	public String toString() {
		return "Config [generalConfig=" + generalConfig + ", kafkaConfig=" + kafkaConfig + ", hiveConfig=" + hiveConfig
				+ "]";
	}

	public GeneralConfig general() {
		return generalConfig;
	}

	public KafkaConfig kafka() {
		return kafkaConfig;
	}

	public HiveConfig hive() {
		return hiveConfig;
	}

	public static Config load(Properties properties) {

		if (INSTANCE != null) {
			throw new IllegalStateException("configuration already loaded");
		}

		Integer geohashPrecisionMin = Integer.parseInt(properties.getProperty("geohashPrecisionMin"));
		Integer geohashPrecisionMax = Integer.parseInt(properties.getProperty("geohashPrecisionMax"));
		String geoApiKey = properties.getProperty("geoApiKey");
		GeneralConfig generalConfig = new GeneralConfig(geohashPrecisionMin, geohashPrecisionMax, geoApiKey);

		String kafkaHostname = properties.getProperty("kafkaHostname");
		Integer kafkaPort = Integer.parseInt(properties.getProperty("kafkaPort"));
		String kafkaInputTopicWeather = properties.getProperty("kafkaInputTopicWeather");
		String kafkaInputTopicHotels = properties.getProperty("kafkaInputTopicHotels");
		String kafkaOutputTopic = properties.getProperty("kafkaOutputTopic");
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaHostname, kafkaPort, kafkaInputTopicWeather,
				kafkaInputTopicHotels, kafkaOutputTopic);

		String hiveHostname = properties.getProperty("hiveHostname");
		Integer hivePort = Integer.parseInt(properties.getProperty("hivePort"));
		String hiveInputTable = properties.getProperty("hiveInputTable");
		HiveConfig hiveConfig = new HiveConfig(hiveHostname, hivePort, hiveInputTable);

		INSTANCE = new Config(generalConfig, kafkaConfig, hiveConfig);
		System.out.println("config:" + INSTANCE);
		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}

	static class GeneralConfig {
		private final Range geohashPrecision;
		private final String geoApiKey;

		public GeneralConfig(Integer geohashPrecisionMin, Integer geohashPrecisionMax, String geoApiKey) {
			this.geohashPrecision = Range.of(geohashPrecisionMin, geohashPrecisionMax);
			this.geoApiKey = geoApiKey;
		}

		public Range getGeohashPrecision() {
			return geohashPrecision;
		}

		public String getGeoApiKey() {
			return geoApiKey;
		}

		@Override
		public String toString() {
			return "GeneralConfig [geohashPrecision=" + geohashPrecision + ", geoApiKey=" + geoApiKey + "]";
		}

	}

	static class KafkaConfig {
		private final String hostname;
		private final Integer port;
		private final String inputTopicWeather;
		private final String inputTopicHotels;
		private final String outputTopic;

		public KafkaConfig(String hostname, Integer port, String inputTopicWeather, String inputTopicHotels,
				String outputTopic) {
			this.hostname = hostname;
			this.port = port;
			this.inputTopicWeather = inputTopicWeather;
			this.inputTopicHotels = inputTopicHotels;
			this.outputTopic = outputTopic;
		}

		public String getHostname() {
			return hostname;
		}

		public Integer getPort() {
			return port;
		}

		public String getInputTopicWeather() {
			return inputTopicWeather;
		}

		public String getInputTopicHotels() {
			return inputTopicHotels;
		}

		public String getOutputTopic() {
			return outputTopic;
		}

		@Override
		public String toString() {
			return "KafkaConfig [hostname=" + hostname + ", port=" + port + ", inputTopicWeather=" + inputTopicWeather
					+ ", inputTopicHotels=" + inputTopicHotels + ", outputTopic=" + outputTopic + "]";
		}

	}

	static class HiveConfig {
		private final String hostname;
		private final Integer port;
		private final String inputTable;

		public HiveConfig(String hostname, Integer port, String inputTable) {
			this.hostname = hostname;
			this.port = port;
			this.inputTable = inputTable;
		}

		public String getHostname() {
			return hostname;
		}

		public Integer getPort() {
			return port;
		}

		public String getInputTable() {
			return inputTable;
		}

		@Override
		public String toString() {
			return "HiveConfig [hostname=" + hostname + ", port=" + port + ", inputTable=" + inputTable + "]";
		}
	}
}
