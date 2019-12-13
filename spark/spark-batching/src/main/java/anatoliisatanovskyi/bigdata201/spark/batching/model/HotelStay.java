package anatoliisatanovskyi.bigdata201.spark.batching.model;

import org.apache.spark.sql.Row;

public class HotelStay {

	private final Long hotelId;
	private final String visitDate;
	private final HotelData hotelData;

	public HotelStay(Row row, HotelData hotelData) {
		this(row.getLong(0), row.getString(1), hotelData);
	}

	public HotelStay(Long hotelId, String visitDate, HotelData hotelData) {
		this.hotelId = hotelId;
		this.visitDate = visitDate;
		this.hotelData = hotelData;
	}

	public Long getHotelId() {
		return hotelId;
	}

	public String getVisitDate() {
		return visitDate;
	}

	public HotelData getHotelData() {
		return hotelData;
	}
}
