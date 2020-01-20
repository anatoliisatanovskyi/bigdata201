package anatoliisatanovskyi.bigdata201.spark.batching;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class DatasetProviderTest extends AbstractTestBase {

	@Test
	public void testGetKafkaHotels() throws Exception {
		Dataset<Row> dataset = dsp().getKafkaHotels(spark());
		assertEquals(2494, dataset.count());
	}

	@Test
	public void testGetAvroExpedia() throws Exception {
		Dataset<Row> dataset = dsp().getAvroExpedia(spark());
		assertEquals(100773, dataset.count());
	}

}
