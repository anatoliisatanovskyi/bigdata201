package anatoliisatanovskyi.bigdata201.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;

import anatoliisatanovskyi.bigdata201.kafka.model.Weather;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherDeserializer;
import anatoliisatanovskyi.bigdata201.kafka.serialization.WeatherSerializer;

public class KafkaLoader {

	private final static String TOPIC = "kafka-weather";
	private final static String BOOTSTRAP_SERVERS = "sandbox-hdp.hortonworks.com:6667";

	public static void main(String[] args) {
		try {
			KafkaLoader kafkaLoader = new KafkaLoader();
			kafkaLoader.runConsumer();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.exit(0);
		}
	}

	public void runConsumer() throws InterruptedException {

		final Consumer<byte[], Weather> consumer = createConsumer();
		System.out.println("Consumer created and subscribed to " + consumer.subscription());

		final int giveUp = 20;
		int noRecordsCount = 0;
		int recordsCount = 0;
		while (true) {

			final ConsumerRecords<byte[], Weather> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			} else {
				recordsCount += consumerRecords.count();
			}

			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record: %s\n", record.value());
			});
			consumer.commitAsync();

			if (recordsCount >= 100) {
				break;
			}
		}

		consumer.close();
		System.out.println("DONE. Records read:" + recordsCount);
	}

	private Consumer<byte[], Weather> createConsumer() {
		final Consumer<byte[], Weather> consumer = new KafkaConsumer<>(createProperties());
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	private Properties createProperties() {
		final Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		// props.put("group.id", "test");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", BytesDeserializer.class.getName());
		props.put("value.serializer", WeatherSerializer.class.getName());
		props.put("value.deserializer", WeatherDeserializer.class.getName());
		return props;
	}
}
