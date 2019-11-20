package anatoliisatanovskyi.bigdata201.commons;

import ch.hsr.geohash.GeoHash;

public class Geohash {
	private final Double lat;
	private final Double lng;

	public Geohash(Double lat, Double lng) {
		this.lat = lat;
		this.lng = lng;
	}

	public String geohash(int precision) {
		return lat != null && lng != null ? GeoHash.withCharacterPrecision(lat, lng, precision).toBase32() : null;
	}

	public Double getLat() {
		return lat;
	}

	public Double getLng() {
		return lng;
	}

}
