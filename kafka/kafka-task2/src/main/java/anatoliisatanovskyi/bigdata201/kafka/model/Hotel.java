package anatoliisatanovskyi.bigdata201.kafka.model;

import java.util.Optional;

import anatoliisatanovskyi.bigdata201.commons.Geohash;
import anatoliisatanovskyi.bigdata201.commons.Geohashable;

public class Hotel implements Geohashable {

	private String name;
	private String country;
	private String city;
	private String address;
	private String latitude;
	private String longitude;
	private String geohash;

	public Hotel() {
	}

	public Hotel(String name, String country, String city, String address, String latitude, String longitude,
			String geohash) {
		this.name = name;
		this.country = country;
		this.city = city;
		this.address = address;
		this.latitude = latitude;
		this.longitude = longitude;
		this.geohash = geohash;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getGeohash() {
		return geohash;
	}

	public void setGeohash(String geohash) {
		this.geohash = geohash;
	}

	@Override
	public String geohash(int precision) {
		try {
			return Optional.ofNullable(geohash).orElse(
					new Geohash(Double.parseDouble(latitude), Double.parseDouble(longitude)).geohash(precision));
		} catch (Exception e) {
			return null;
		}
	}
}
