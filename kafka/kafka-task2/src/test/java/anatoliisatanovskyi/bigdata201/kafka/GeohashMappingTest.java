package anatoliisatanovskyi.bigdata201.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import anatoliisatanovskyi.bigdata201.kafka.model.Hotel;

public class GeohashMappingTest {
	@Test
	public void testMatchExact() throws Exception {
		GeohashMapping<Hotel> geohashMapping = new GeohashMapping<>();
		String geohash1 = "12345";
		Hotel hotel1 = new Hotel("1", "stub1", "stub1", "stub1", "stub1", "1", "1", geohash1);
		geohashMapping.store(hotel1, Range.of(3, 5));

		String geohash2 = "67890";
		Hotel hotel2 = new Hotel("2", "stub2", "stub2", "stub2", "stub2", "2", "2", geohash2);
		geohashMapping.store(hotel2, Range.of(3, 5));

		Hotel search = new Hotel("1", "stub1", "stub1", "stub1", "stub1", "1", "1", geohash1);
		GeohashMapping<Hotel>.MatchResult match = geohashMapping.match(search, Range.of(5, 5));
		assertEquals(Integer.valueOf(5), match.getPrecision());
		assertNotNull(match.getItem());
		assertEquals(hotel1.getName(), match.getItem().getName());
		assertEquals(hotel1.getGeohash(), match.getItem().getGeohash());
	}

	@Test
	public void testMatchLess() throws Exception {
		GeohashMapping<Hotel> geohashMapping = new GeohashMapping<>();
		String geohash1 = "12345";
		Hotel hotel1 = new Hotel("1", "stub1", "stub1", "stub1", "stub1", "1", "1", geohash1);
		geohashMapping.store(hotel1, Range.of(3, 5));

		String geohash2 = "67890";
		Hotel hotel2 = new Hotel("2", "stub2", "stub2", "stub2", "stub2", "2", "2", geohash2);
		geohashMapping.store(hotel2, Range.of(3, 5));

		String searchGeohash = "123";
		Hotel search = new Hotel("1", "stub1", "stub1", "stub1", "stub1", "1", "1", searchGeohash);
		GeohashMapping<Hotel>.MatchResult match = geohashMapping.match(search, Range.of(3, 5));
		assertEquals(Integer.valueOf(3), match.getPrecision());
		assertNotNull(match.getItem());
		assertEquals(hotel1.getName(), match.getItem().getName());
		assertEquals(hotel1.getGeohash(), match.getItem().getGeohash());
	}
}
