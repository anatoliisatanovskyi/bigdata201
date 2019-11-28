package anatoliisatanovskyi.bigdata201.kafka;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import anatoliisatanovskyi.bigdata201.kafka.model.Hotel;
import anatoliisatanovskyi.bigdata201.kafka.model.HotelWeather;
import anatoliisatanovskyi.bigdata201.kafka.model.Weather;

public class HotelsWeatherEnricher {

	private static final Logger logger = LoggerFactory.getLogger(HotelsWeatherEnricher.class);

	private Config config = Config.instance();

	private KafkaProperties kafkaProperties = new KafkaProperties();

	private ExecutorService executorService;
	private CountDownLatch workerFinishLatch;
	private ScheduledExecutorService statisticsService = Executors.newSingleThreadScheduledExecutor();
	private GeohashMapping<Hotel> geohashMapping;

	private AtomicInteger consumeCounter = new AtomicInteger();
	private AtomicInteger produceCounter = new AtomicInteger();

	public void init() {
		executorService = Executors.newFixedThreadPool(config.kafka().getInputTopicWeatherPartitions());
		workerFinishLatch = new CountDownLatch(config.kafka().getInputTopicWeatherPartitions());
		statisticsService.scheduleAtFixedRate(new StatisticsTimer(), 0L, 5L, TimeUnit.SECONDS);
	}

	public void awaitTillFinished() throws InterruptedException {
		workerFinishLatch.await();
	}

	public void prepareHotelsData() throws SQLException, IOException {
		List<Hotel> hotels = readHotelsDataFromKafka();
		this.geohashMapping = mapByGeohash(hotels);
		logger.info("loaded " + this.geohashMapping.size() + " hotel mappings");
	}

	private List<Hotel> readHotelsDataFromKafka() {
		List<Hotel> list = new ArrayList<>();
		try (Consumer<byte[], Hotel> consumer = new KafkaConsumer<>(kafkaProperties.getHotelProperties())) {
			consumer.subscribe(Collections.singletonList(config.kafka().getInputTopicHotels()));
			ConsumerRecords<byte[], Hotel> records = null;
			do {
				records = consumer.poll(Duration.ofSeconds(10));
				records.forEach(record -> {
					Hotel item = record.value();
					if (item.notEmpty()) {
						list.add(item);
					}
				});
				consumer.commitAsync();
			} while (records.count() > 0);
		}
		return list;
	}

	private GeohashMapping<Hotel> mapByGeohash(List<Hotel> hotels) {
		GeohashMapping<Hotel> geohashMapping = new GeohashMapping<>();
		hotels.stream().filter(h -> h.getGeohash() != null)
				.forEach(hotel -> geohashMapping.store(hotel, config.general().getGeohashPrecision()));
		return geohashMapping;
	}

	public void consumeWeatherProduceEnriched() {
		if (geohashMapping == null || geohashMapping.isEmpty()) {
			throw new IllegalStateException("geohash mapping is not loaded");
		}
		IntStream.range(0, config.kafka().getInputTopicWeatherPartitions())
				.forEach(partition -> executorService.submit(new Worker(partition)));
	}

	public void terminate() {
		if (executorService != null) {
			shutdownKafkaThreads();
		}
		if (statisticsService != null) {
			shutdownStatisticsThreads();
		}
	}

	private void shutdownStatisticsThreads() {
		statisticsService.shutdown();
		try {
			statisticsService.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("an error occured during statistics thread termination: " + e.getMessage(), e);
		}
		statisticsService.shutdownNow();
	}

	private void shutdownKafkaThreads() {
		executorService.shutdown();
		try {
			executorService.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("an error occured during threads termination: " + e.getMessage(), e);
		}
		executorService.shutdownNow();
	}

	private class Worker implements Runnable {

		private Integer partition;

		public Worker(Integer partition) {
			this.partition = partition;
		}

		@Override
		public void run() {
			try (Consumer<byte[], Weather> consumer = new KafkaConsumer<>(kafkaProperties.getWeatherProperties());
					KafkaProducer<byte[], HotelWeather> producer = new KafkaProducer<byte[], HotelWeather>(
							kafkaProperties.getHotelWeatherProperties())) {
				consumer.assign(Collections
						.singletonList(new TopicPartition(config.kafka().getInputTopicWeather(), partition)));
				ConsumerRecords<byte[], Weather> records = null;
				do {
					records = consumer.poll(Duration.ofSeconds(10));
					records.forEach(record -> {
						Weather weather = record.value();
						consumeCounter.incrementAndGet();

						HotelWeather enriched = enrichData(weather);
						if (enriched != null) {
							produceData(producer, enriched);
							produceCounter.incrementAndGet();
						}
					});
					consumer.commitAsync();
				} while (records.count() > 0);
			} catch (Exception e) {
				logger.error("worker received an error: " + e.getMessage(), e);
			} finally {
				workerFinishLatch.countDown();
			}
		}

		private HotelWeather enrichData(Weather currWeather) {
			GeohashMapping<Hotel>.MatchResult matchResult = geohashMapping.match(currWeather,
					config.general().getGeohashPrecision());
			HotelWeather enriched = null;
			if (matchResult.getItem() != null) {
				enriched = new HotelWeather(matchResult.getItem(), currWeather, matchResult.getPrecision());
			}
			return enriched;
		}

		private void produceData(KafkaProducer<byte[], HotelWeather> producer, HotelWeather enriched) {
			ProducerRecord<byte[], HotelWeather> record = new ProducerRecord<byte[], HotelWeather>(
					config.kafka().getOutputTopic(), enriched);
			producer.send(record);
		}
	}

	private class StatisticsTimer implements Runnable {

		private int currConcumed;
		private int currProduced;

		@Override
		public void run() {
			if (doRun()) {
				currConcumed = consumeCounter.get();
				currProduced = produceCounter.get();
				logger.info("consumed records: " + currConcumed + " produced records: " + currProduced);
			}
		}

		boolean doRun() {
			return consumeCounter.get() != currConcumed || produceCounter.get() != currProduced;
		}
	}
}
