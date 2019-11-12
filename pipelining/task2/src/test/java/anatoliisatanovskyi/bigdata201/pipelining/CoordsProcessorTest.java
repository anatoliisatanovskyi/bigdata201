package anatoliisatanovskyi.bigdata201.pipelining;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class CoordsProcessorTest {

	@Test
	public void testWithCorrectCoords() throws Exception {

		String apiKey = "91689528cebb4e1d9f309c1038119cf7";
		int geohashLength = 5;
		CoordsProcessor ñoordsProcessor = new CoordsProcessor(apiKey, geohashLength);

		String lat = "41.70064";
		String lng = "-91.607";

		String geohash = ñoordsProcessor.processToGeohash(lat, lng);
		assertNotNull(geohash);
		assertEquals("9zqv5", geohash);
	}

	@Test
	public void testWithApiCallCoords() throws Exception {

		String apiKey = "91689528cebb4e1d9f309c1038119cf7";
		int geohashLength = 5;
		CoordsProcessor coordsProcessor = new CoordsProcessor(apiKey, geohashLength);

		String country = "US";
		String city = "Herndon";
		String address = "2300 Dulles Corner Blvd";

		String geohash = coordsProcessor.processToGeohash(country, city, address);
		assertNotNull(geohash);
		assertEquals("sygtw", geohash);
	}
}
