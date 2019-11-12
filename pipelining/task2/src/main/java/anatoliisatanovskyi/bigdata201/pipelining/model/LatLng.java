package anatoliisatanovskyi.bigdata201.pipelining.model;

import ch.hsr.geohash.GeoHash;

public class LatLng {

	private final Double lat;
	private final Double lng;

	public LatLng(Double lat, Double lng) {
		this.lat = lat;
		this.lng = lng;
	}

	public LatLng(String latStr, String lngStr) {
		this(parseCoord(latStr), parseCoord(lngStr));
	}

	private static Double parseCoord(String value) {
		Double coord = null;
		if (value != null && !value.isEmpty()) {
			try {
				coord = Double.valueOf(value);
			} catch (NumberFormatException e) {
			}
		}
		return coord;
	}

	public boolean validate() {
		return lat != null && lng != null;
	}

	public String geohash(int precision) {
		return GeoHash.withCharacterPrecision(lat, lng, precision).toBase32();
	}

	public Double getLat() {
		return lat;
	}

	public Double getLng() {
		return lng;
	}
}
