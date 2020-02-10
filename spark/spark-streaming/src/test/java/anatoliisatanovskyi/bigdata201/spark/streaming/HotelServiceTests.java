package anatoliisatanovskyi.bigdata201.spark.streaming;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import anatoliisatanovskyi.bigdata201.spark.streaming.service.HotelService;

public class HotelServiceTests extends AbstractTestBase {

	private HotelService hotelService = new HotelService();

	@Test
	public void testEnrichWithWeather() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		assertEquals(9, ds.count());
	}

	@Test
	public void testFilterTemperatureGreaterThen() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		assertEquals(8, ds.count());
	}

	@Test
	public void testEnrichWithStayDuration() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		ds = hotelService.enrichWithStayDuration(ds);
		assertEquals(8, ds.count());
	}

	@Test
	public void testEnrichWithChildrenPresent() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		ds = hotelService.enrichWithStayDuration(ds);
		ds = hotelService.enrichWithChildrenPresent(ds);
		assertEquals(8, ds.count());
	}

	@Test
	public void testCalculateCustomerPreferenceChildrenFilterOff() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		ds = hotelService.enrichWithStayDuration(ds);
		ds = hotelService.enrichWithChildrenPresent(ds);
		ds = hotelService.calculateCustomerPreferenceWithChildren(ds);
		assertEquals(1, ds.count());
	}

	@Test
	public void testCalculateCustomerPreferenceChildrenFilterOn() {
		Dataset<Row> expedia = dsp().getExpedia(spark(), 2016);
		Dataset<Row> hotelWeather = dsp().getHotelWeather(spark());
		Dataset<Row> ds = hotelService.enrichWithWeather(expedia, hotelWeather);
		ds = hotelService.filterTemperatureGreaterThen(ds, 0);
		ds = hotelService.enrichWithStayDuration(ds);
		ds = hotelService.enrichWithChildrenPresent(ds);
		ds = hotelService.calculateCustomerPreferenceWithoutChildren(ds);
		assertEquals(7, ds.count());
	}
	
}
