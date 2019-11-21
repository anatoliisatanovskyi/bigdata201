package anatoliisatanovskyi.bigdata201.kafka;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;

import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelSerializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelWeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.HotelWeatherSerializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherSerializer;

public class KafkaProperties {

	private final Config config = Config.instance();
	
	public Properties getWeatherProperties() {
		Properties props = createProperties();
		props.put("value.serializer", WeatherSerializer.class.getName());
		props.put("value.deserializer", WeatherDeserializer.class.getName());
		return props;
	}

	public Properties getHotelProperties() {
		Properties props = createProperties();
		props.put("value.serializer", HotelSerializer.class.getName());
		props.put("value.deserializer", HotelDeserializer.class.getName());
		return props;
	}

	public Properties getHotelWeatherProperties() {
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
	
}
