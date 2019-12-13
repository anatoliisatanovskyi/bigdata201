package anatoliisatanovskyi.bigdata201.spark.batching.model;

import com.fasterxml.jackson.annotation.JsonGetter;

public class Hotel {

	private String id;
	private String name;
	private String country;
	private String city;
	private String address;

	@JsonGetter("Id")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@JsonGetter("Name")
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@JsonGetter("Country")
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@JsonGetter("City")
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@JsonGetter("Address")
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
