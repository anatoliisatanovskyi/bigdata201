package anatoliisatanovskyi.bigdata201.pipelining;

import static org.junit.Assert.*;

import org.junit.Test;

import anatoliisatanovskyi.bigdata201.pipelining.model.LatLng;

public class LatLngTest {

	@Test
	public void testParseNull() throws Exception {
		LatLng latLng = new LatLng(null, "40.00");
		assertFalse(latLng.validate());
	}

	@Test
	public void testParseEmpty() throws Exception {
		LatLng latLng = new LatLng("40.00", "");
		assertFalse(latLng.validate());
	}

	@Test
	public void testParseNA() throws Exception {
		LatLng latLng = new LatLng("N/A", "N/A");
		assertFalse(latLng.validate());
	}

	@Test
	public void testParseInvalidAlphabetic() throws Exception {
		LatLng latLng = new LatLng("40.00", "a40.00");
		assertFalse(latLng.validate());
	}

	@Test
	public void testParseInvalidCharacter() throws Exception {
		LatLng latLng = new LatLng("40.00", "4/0.00");
		assertFalse(latLng.validate());
	}

	@Test
	public void testParseValidPositive() throws Exception {
		LatLng latLng = new LatLng("40.577811", "122.357688");
		assertTrue(latLng.validate());
	}

	@Test
	public void testParseValidNegative() throws Exception {
		LatLng latLng = new LatLng("38.849447", "-77.050946");
		assertTrue(latLng.validate());
	}

	@Test
	public void testGenerateGeohash() throws Exception {
		LatLng latLng = new LatLng("38.849447", "-77.050946");
		assertEquals("dqcjn", latLng.geohash(5));
	}
}
