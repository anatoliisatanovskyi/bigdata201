package anatoliisatanovskyi.bigdata201.kafka.model;

import anatoliisatanovskyi.bigdata201.commons.Geohash;
import anatoliisatanovskyi.bigdata201.commons.Geohashable;

public class Weather implements Geohashable {
	
	private Double lng;
	private Double lat;
	private Double avg_tmpr_f;
	private Double avg_tmpr_c;
	private String wthr_date;

	@Override
	public String geohash(int precision) {
		return new Geohash(lat, lng).geohash(precision);
	}
	
	@Override
	public String toString() {
		return "Weather [lng=" + lng + ", lat=" + lat + ", avg_tmpr_f=" + avg_tmpr_f + ", avg_tmpr_c=" + avg_tmpr_c
				+ ", wthr_date=" + wthr_date + "]";
	}

	public Double getLng() {
		return lng;
	}

	public void setLng(Double lng) {
		this.lng = lng;
	}

	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
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
}
