package anatoliisatanovskyi.bigdata201.kafka.model;

import java.util.Optional;

public class HotelWeather {

	private String name;
	private String country;
	private String city;
	private String addres;
	private Double avg_tmpr_f;
	private Double avg_tmpr_c;
	private String wthr_date;
	private Integer precision;

	public HotelWeather() {
	}

	public HotelWeather(Hotel hotel, Weather weather, Integer precision) {
		this(Optional.ofNullable(hotel).map(Hotel::getName).orElse(null), //
				Optional.ofNullable(hotel).map(Hotel::getCountry).orElse(null), //
				Optional.ofNullable(hotel).map(Hotel::getCity).orElse(null), //
				Optional.ofNullable(hotel).map(Hotel::getAddress).orElse(null), //
				Optional.ofNullable(weather).map(Weather::getAvg_tmpr_f).orElse(null), //
				Optional.ofNullable(weather).map(Weather::getAvg_tmpr_c).orElse(null), //
				Optional.ofNullable(weather).map(Weather::getWthr_date).orElse(null), //
				precision);
	}

	public HotelWeather(String name, String country, String city, String addres, Double avg_tmpr_f, Double avg_tmpr_c,
			String wthr_date, Integer precision) {
		this.name = name;
		this.country = country;
		this.city = city;
		this.addres = addres;
		this.avg_tmpr_f = avg_tmpr_f;
		this.avg_tmpr_c = avg_tmpr_c;
		this.wthr_date = wthr_date;
		this.precision = precision;
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

	public String getAddres() {
		return addres;
	}

	public void setAddres(String addres) {
		this.addres = addres;
	}

	public Double getAvg_tmpr_f() {
		return avg_tmpr_f;
	}

	public void setAvg_tmpr_f(Double avg_tmpr_f) {
		this.avg_tmpr_f = avg_tmpr_f;
	}

	public Double getAvg_tmpr_c() {
		return avg_tmpr_c;
	}

	public void setAvg_tmpr_c(Double avg_tmpr_c) {
		this.avg_tmpr_c = avg_tmpr_c;
	}

	public String getWthr_date() {
		return wthr_date;
	}

	public void setWthr_date(String wthr_date) {
		this.wthr_date = wthr_date;
	}

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}
}
