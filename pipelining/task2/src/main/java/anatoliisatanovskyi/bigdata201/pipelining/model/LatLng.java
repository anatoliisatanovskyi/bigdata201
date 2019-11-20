package anatoliisatanovskyi.bigdata201.pipelining.model;

import anatoliisatanovskyi.bigdata201.commons.Geohash;
import anatoliisatanovskyi.bigdata201.commons.Geohashable;

public class LatLng implements Geohashable {

	private final Geohash geohash;

	public LatLng(Double lat, Double lng) {
		this.geohash = new Geohash(lat, lng);
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
		return geohash.getLat() != null && geohash.getLng() != null;
	}

	public Double getLat() {
		return geohash.getLat();
	}

	public Double getLng() {
		return geohash.getLng();
	}

	@Override
	public String geohash(int precision) {
		return geohash.geohash(precision);
	}
}
