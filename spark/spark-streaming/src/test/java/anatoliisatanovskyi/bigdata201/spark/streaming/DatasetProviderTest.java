package anatoliisatanovskyi.bigdata201.spark.streaming;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class DatasetProviderTest extends AbstractTestBase {

	@Test
	public void testGetKafkaHotels() throws Exception {
		Dataset<Row> dataset = dsp().getHotelWeather(spark());
		assertEquals(5, dataset.count());
	}

	@Test
	public void testGetAvroExpedia() throws Exception {
		Dataset<Row> dataset = dsp().getExpedia(spark(), 2016);
		assertEquals(100773, dataset.count());
	}

}
