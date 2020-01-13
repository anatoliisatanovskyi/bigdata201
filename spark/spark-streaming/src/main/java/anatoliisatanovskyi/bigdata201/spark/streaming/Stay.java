package anatoliisatanovskyi.bigdata201.spark.streaming;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang.StringUtils;

public enum Stay {

	// "Erroneous data": null, more than month(30 days), less than or equal to 0
	// "Short stay": 1 day stay
	// "Standart stay": 2-7 days
	// "Standart extended stay": 1-2 weeks
	// "Long stay": 2-4 weeks (less than month)

	ERRONEOUS, SHORT, STANDART, STANDART_EXTENDED, LONG;

	public static Stay calculate(String fromDate, String toDate) {

		if (StringUtils.isEmpty(fromDate) || StringUtils.isEmpty(toDate)) {
			return ERRONEOUS;
		}

		LocalDate from = LocalDate.parse(fromDate);
		LocalDate to = LocalDate.parse(toDate);

		long diff = ChronoUnit.DAYS.between(from, to);
		if (diff == 1) {
			return SHORT;
		} else if (diff < 8) {
			return STANDART;
		} else if (diff < 15) {
			return STANDART_EXTENDED;
		} else if (diff < 31) {
			return LONG;
		} else {
			return ERRONEOUS;
		}
	}

}
