package anatoliisatanovskyi.bigdata201.spark.batching.model;

import org.apache.spark.sql.Row;

public class HotelData {
	private final Long id;
	private final String name;
	private final String country;
	private final String city;
	private final String address;

	public HotelData(Row row) {
		this(row.getLong(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4));
	}

	public HotelData(Long id, String name, String country, String city, String address) {
		this.id = id;
		this.name = name;
		this.country = country;
		this.city = city;
		this.address = address;
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getCountry() {
		return country;
	}

	public String getCity() {
		return city;
	}

	public String getAddress() {
		return address;
	}
}
