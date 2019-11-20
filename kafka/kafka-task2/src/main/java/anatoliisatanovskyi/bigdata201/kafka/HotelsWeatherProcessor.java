package anatoliisatanovskyi.bigdata201.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;

import anatoliisatanovskyi.bigdata201.kafka.model.Hotel;
import anatoliisatanovskyi.bigdata201.kafka.model.HotelWeather;
import anatoliisatanovskyi.bigdata201.kafka.model.Weather;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelSerializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelWeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelWeatherSerializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherSerializer;

public class HotelsWeatherProcessor {

	private final Config config = Config.instance();
	private GeohashMapping<Hotel> geohashMapping = new GeohashMapping<>();

	private AtomicInteger pCounter = new AtomicInteger();
	private AtomicInteger cCounter = new AtomicInteger();

	public static void main(String[] args) {
		try {

			if (args.length == 0) {
				throw new IllegalArgumentException("configuration filepath is required");
			}
			String configFilepath = args[0];
			loadConfiguration(configFilepath);

			HotelsWeatherProcessor hotelsWeatherProcessor = new HotelsWeatherProcessor();
			hotelsWeatherProcessor.readAndMapHotelsData();
			hotelsWeatherProcessor.consumeEnrichAndProduceData();
		} catch (Exception e) {
			System.err.println("an error occured: " + e.getMessage());
			e.printStackTrace();
		} finally {
			System.exit(0);
		}
	}

	void readAndMapHotelsData() throws SQLException, IOException {
		List<Hotel> hotels = readHotelsDataFromKafka();
		this.geohashMapping = mapByGeohash(hotels);
	}

	List<Hotel> readHotelsDataFromKafka() {
		List<Hotel> list = new ArrayList<>();
		try (Consumer<byte[], Hotel> consumer = new KafkaConsumer<>(createHotelProperties())) {
			consumer.subscribe(Collections.singletonList(config.kafka().getInputTopicHotels()));
			ConsumerRecords<byte[], Hotel> records = null;
			do {
				records = consumer.poll(Duration.ofSeconds(10));
				records.forEach(record -> list.add(record.value()));
				consumer.commitAsync();
			} while (records.count() > 0);
		}
		return list;
	}

	List<Hotel> readHotelsDataFromHive() throws SQLException, IOException {
		try (HiveClient hiveClient = new HiveClient()) {
			hiveClient.connect();
			return hiveClient.extract();
		}
	}

	private GeohashMapping<Hotel> mapByGeohash(List<Hotel> hotels) {
		GeohashMapping<Hotel> geohashMapping = new GeohashMapping<>();
		hotels.stream().filter(h -> h.getGeohash() != null)
				.forEach(hotel -> geohashMapping.store(hotel, config.general().getGeohashPrecision()));
		System.out.println("mapped hotels:" + geohashMapping.size());
		return geohashMapping;
	}

	void consumeEnrichAndProduceData() {
		try (Consumer<byte[], Weather> consumer = new KafkaConsumer<>(createWeatherProperties());
				KafkaProducer<byte[], HotelWeather> producer = new KafkaProducer<byte[], HotelWeather>(
						createHotelWeatherProperties())) {
			consumer.subscribe(Collections.singletonList(config.kafka().getInputTopicWeather()));
			ConsumerRecords<byte[], Weather> records = null;
			Set<Integer> partitions = new HashSet<>();
			do {
				records = consumer.poll(Duration.ofSeconds(10));
				records.forEach(record -> {
					Weather weather = record.value();
					partitions.add(record.partition());
					if (cCounter.incrementAndGet() % 100000 == 0) {
						System.out.println("consumed " + cCounter.get() + " used_partitions=" + partitions);
					}
					HotelWeather enriched = enrichData(weather);
					produceData(producer, enriched);
				});
				consumer.commitAsync();
			} while (records.count() > 0);
		}
	}

	private Properties createWeatherProperties() {
		Properties props = createProperties();
		props.put("value.serializer", WeatherSerializer.class.getName());
		props.put("value.deserializer", WeatherDeserializer.class.getName());
		return props;
	}

	private Properties createHotelProperties() {
		Properties props = createProperties();
		props.put("value.serializer", HotelSerializer.class.getName());
		props.put("value.deserializer", HotelDeserializer.class.getName());
		return props;
	}

	private Properties createHotelWeatherProperties() {
		Properties props = createProperties();
		props.put("value.serializer", HotelWeatherSerializer.class.getName());
		props.put("value.deserializer", HotelWeatherDeserializer.class.getName());
		return props;
	}

	private Properties createProperties() {
		final Properties props = new Properties();
		props.put("bootstrap.servers", String.format("%s:%d", config.kafka().getHostname(), config.kafka().getPort()));
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.serializer", BytesSerializer.class.getName());
		props.put("key.deserializer", BytesDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	private HotelWeather enrichData(Weather currWeather) {
		GeohashMapping<Hotel>.Result find = geohashMapping.match(currWeather, config.general().getGeohashPrecision());
		return new HotelWeather(find.getItem(), currWeather, find.getPrecision());
	}

	private void produceData(KafkaProducer<byte[], HotelWeather> producer, HotelWeather enriched) {
		ProducerRecord<byte[], HotelWeather> record = new ProducerRecord<byte[], HotelWeather>(
				config.kafka().getOutputTopic(), enriched);
		producer.send(record);

		if (pCounter.incrementAndGet() % 100000 == 0) {
			System.out.println("produced " + pCounter.get());
		}
	}

	public static void loadConfiguration(String configFilepath) throws IOException {
		try (FileInputStream fis = new FileInputStream(new File(configFilepath))) {
			Properties properties = new Properties();
			properties.load(fis);
			Config.load(properties);
		}
	}
}
