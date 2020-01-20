package anatoliisatanovskyi.bigdata201.spark.batching;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import anatoliisatanovskyi.bigdata201.spark.batching.service.HotelService;

public class HotelServiceTests extends AbstractTestBase {

	private HotelService hotelService = new HotelService();

	@Test
	public void testGetExpediaIncludingIdleDays() {
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);
		assertEquals(100773, expediaIncludingIdleDays.count());
	}

	@Test
	public void testGetIdleHotels() throws Exception {

		Dataset<Row> hotels = dsp().getKafkaHotels(spark());
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);

		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);
		assertEquals(48196, idleHotels.count());
	}

	@Test
	public void testGetValidHotels() throws Exception {
		Dataset<Row> hotels = dsp().getKafkaHotels(spark());
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);
		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);

		Dataset<Row> validHotels = hotelService.getValidHotels(hotels, idleHotels);
		assertEquals(1, validHotels.count());
	}

	@Test
	public void testGetExpediaWithValidHotels() throws Exception {
		Dataset<Row> hotels = dsp().getKafkaHotels(spark());
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);
		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);
		Dataset<Row> validHotels = hotelService.getValidHotels(hotels, idleHotels);

		Dataset<Row> expediaWithValidHotels = hotelService.getExpediaWithValidHotels(expediaIncludingIdleDays,
				validHotels);
		assertEquals(0, expediaWithValidHotels.count());
	}

	@Test
	public void testCountByCountry() {
		Dataset<Row> hotels = dsp().getKafkaHotels(spark());
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);
		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);
		Dataset<Row> validHotels = hotelService.getValidHotels(hotels, idleHotels);

		Dataset<Row> expediaWithValidHotels = hotelService.getExpediaWithValidHotels(expediaIncludingIdleDays,
				validHotels);

		Dataset<Row> groupByCountryCount = hotelService.groupByColumnCount(expediaWithValidHotels, "Country");
		assertEquals(0, groupByCountryCount.count());
	}

	@Test
	public void testCountByCity() {
		Dataset<Row> hotels = dsp().getKafkaHotels(spark());
		Dataset<Row> expedia = dsp().getAvroExpedia(spark());
		Dataset<Row> expediaIncludingIdleDays = hotelService.getExpediaIncludingIdleDays(expedia);
		Dataset<Row> idleHotels = hotelService.getIdleHotels(hotels, expediaIncludingIdleDays);
		Dataset<Row> validHotels = hotelService.getValidHotels(hotels, idleHotels);

		Dataset<Row> expediaWithValidHotels = hotelService.getExpediaWithValidHotels(expediaIncludingIdleDays,
				validHotels);

		Dataset<Row> groupByCountryCount = hotelService.groupByColumnCount(expediaWithValidHotels, "Country");
		assertEquals(0, groupByCountryCount.count());
	}
}
