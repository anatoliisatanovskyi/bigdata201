package anatoliisatanovskyi.bigdata201.spark.batching;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

public class CustomTest {

	@Test
	public void testIdleBusyDays() throws Exception {
		Long startTimestamp = new GregorianCalendar(2019, 01, 01, 0, 0, 0).getTimeInMillis();
		Map<Long, HotelStayStatistics> statistics = new HashMap<>();
		generateWithConstraints(1000, startTimestamp, startTimestamp + TimeUnit.DAYS.toMillis(60)).forEach(hv -> {
			statistics.putIfAbsent(hv.id, new HotelStayStatistics(hv.id));
			getDateTimestampsBetween(LocalDate.parse(hv.ci), LocalDate.parse(hv.co))
					.forEach(statistics.get(hv.id)::addBusyDay);
		});
	}

	private List<HotelVisit> generateWithConstraints(int count, long startTimestamp, long endTimestamp) {
		return null;
	}

	public Set<Long> getDateTimestampsBetween(LocalDate startDate, LocalDate endDate) {
		return IntStream.iterate(0, i -> i + 1).limit(ChronoUnit.DAYS.between(startDate, endDate))
				.mapToObj(i -> startDate.plusDays(i))
				.map(i -> LocalDateTime.of(i, LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
				.collect(Collectors.toSet());
	}

	class HotelStayStatistics {
		Long hotelId;
		private TreeSet<Long> busyDays = new TreeSet<>();

		HotelStayStatistics(Long hotelId) {
			this.hotelId = hotelId;
		}

		public HotelStayStatistics addBusyDay(Long timestamp) {
			this.busyDays.add(timestamp);
			return this;
		}

		public Set<Long> getBusyDays() {
			return busyDays;
		}

		public Set<Long> getIdleDays() {

			if (busyDays.size() <= 2)
				return Collections.emptySet();

			Long ci = busyDays.first();
			Long co = busyDays.last();

			return getDateTimestampsBetween(LocalDate.parse(new Date(ci).toString()),
					LocalDate.parse(new Date(co).toString())).stream().filter(ts -> !busyDays.contains(ts))
							.collect(Collectors.toSet(TreeSet::new));
		}
	}

	class HotelVisit {
		final Long id;
		final String ci;
		final String co;

		public HotelVisit(Long id, String ci, String co) {
			this.id = id;
			this.ci = ci;
			this.co = co;
		}

		public Long getId() {
			return id;
		}

		public String getCi() {
			return ci;
		}

		public String getCo() {
			return co;
		}
	}

}
