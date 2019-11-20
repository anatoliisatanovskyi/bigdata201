package anatoliisatanovskyi.bigdata201.kafka;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;

import anatoliisatanovskyi.bigdata201.kafka.model.Hotel;
import anatoliisatanovskyi.bigdata201.kafka.model.Weather;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherSerializer;

public class KafkaTest {

	private final static String TOPIC = "kafka_weather";
	private final static String BOOTSTRAP_SERVERS = "sandbox-hdp.hortonworks.com:6667";

	//@Test
	public void testKafkaConsumer() throws Exception {
		runConsumer();
	}

	void runConsumer() throws InterruptedException {

		final Consumer<byte[], Weather> consumer = createConsumer();
		System.out.println("consumer created and subscribed to " + consumer.subscription());

		final int giveUp = 100;
		int noRecordsCount = 0;
		while (true) {

			final ConsumerRecords<byte[], Weather> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset());
			});
			consumer.commitAsync();
		}

		consumer.close();
		System.out.println("DONE");
	}

	private Consumer<byte[], Weather> createConsumer() {

		final Properties props = new Properties();
		/*
		 * props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		 * props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		 * props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		 * ByteArrayDeserializer.class.getName());
		 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		 * StringDeserializer.class.getName());
		 */

		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", BytesDeserializer.class.getName());
		props.put("value.serializer", WeatherSerializer.class.getName());
		props.put("value.deserializer", WeatherDeserializer.class.getName());

		final Consumer<byte[], Weather> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}
}
