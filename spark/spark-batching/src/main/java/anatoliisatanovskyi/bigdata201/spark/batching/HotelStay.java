package anatoliisatanovskyi.bigdata201.spark.batching;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.net.ntp.TimeStamp;

public class HotelStay {

	private Long hotelId;
	private TreeSet<Long> busyDays = new TreeSet<>();

	public HotelStay(Long hotelId) {
		this.hotelId = hotelId;
	}

	public void addBysyPeriod(String begin, String end) {
		busyDays.addAll(getPeriodTimestamps(LocalDate.parse(begin), LocalDate.parse(end)));
	}

	public TreeSet<Long> getBusyDays() {
		return busyDays;
	}

	public TreeSet<Long> getIdleDays() {
		return getPeriodTimestamps(Instant.ofEpochMilli(busyDays.first()).atZone(ZoneId.systemDefault()).toLocalDate(),
				Instant.ofEpochMilli(busyDays.last()).atZone(ZoneId.systemDefault()).toLocalDate()).removeAll(busyDays);
	}

	public int getMaxIdleDays() {
		TreeSet<Long> idleDays = getIdleDays();
		if (idleDays.isEmpty()) {
			return 0;
		}
		int maxIdle = 0;
		long prevTimestamp = 0L;
		for (Long timestamp : idleDays) {
			if (prevTimestamp == 0 || timestamp == prevTimestamp + TimeUnit.DAYS.toMillis(1)) {
				maxIdle++;
			}
			prevTimestamp = timestamp;
		}
		return maxIdle;
	}

	private TreeSet<Long> getPeriodTimestamps(LocalDate begin, LocalDate end) {
		Period period = Period.between(begin, end);
		Long beginTimestamp = begin.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		return IntStream.range(0, period.getDays()).boxed().map(i -> beginTimestamp + TimeUnit.DAYS.toMillis(i))
				.collect(Collectors.toSet(TreeSet::new));
	}
}
