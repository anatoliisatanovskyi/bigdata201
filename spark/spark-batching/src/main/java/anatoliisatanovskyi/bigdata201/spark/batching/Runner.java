package anatoliisatanovskyi.bigdata201.spark.batching;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Runner {

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	private Config config = Config.instance();

	public static void main(String[] args) {

		try {
			loadConfiguration(args);

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

	public void dooStuff() {
		String warehouseLocation = new File(config.getSparkSqlWarehouseDir()).getAbsolutePath();
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();

		Map<Long, HotelStay> hotelStays = new HashMap<>();
		spark.sql("SELECT distinct t1.hotel_id, t1.srch_ci, t1.srch_co, t2.name FROM hdfs_expedia t1\r\n"
				+ "JOIN hotel_weather t2\r\n" + "ON t1.hotel_id=t2.hotelid\r\n"
				+ "ORDER BY t1.hotel_id,t1.srch_ci limit 100;").foreach(row -> {
					Long hotelId = row.getLong(0);
					String ci = row.getString(1);
					String co = row.getString(1);
					hotelStays.putIfAbsent(hotelId, new HotelStay(hotelId));
					hotelStays.get(hotelId).addBysyPeriod(ci, co);
				});
		hotelStays.values().stream().filter(stay -> {
			int maxIdleDays = stay.getMaxIdleDays();
			return maxIdleDays >= config.getMinIdleDays() && maxIdleDays < config.getMaxIdleDays();
		});
	}

	public class HotelStayStatistics {

	}

	public void readKafkaTopic() {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("WordCountingApp");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", String.format("%s:%d", config.kafka().getHostname(), config.kafka().getPort()));
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.serializer", BytesSerializer.class.getName());
		props.put("key.deserializer", BytesDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		Collection<String> topics = Arrays.asList(config.kafka().getTopicHotelWeather());

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, props));
	}
}
