package anatoliisatanovskyi.bigdata201.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import anatoliisatanovskyi.bigdata201.commons.Geohashable;

public class GeohashMapping<G extends Geohashable> {

	private Map<String, List<G>> mapping = new HashMap<>();

	public void store(G hotel, Range precision) {
		String geohash = hotel.geohash(precision.getMax());
		if (geohash != null) {
			int max = precision.getMax() > geohash.length() ? geohash.length() : precision.getMax();
			int min = precision.getMin() > 0 ? precision.getMin() : 1;
			for (int step = max; step >= min; step--) {
				String geohashKey = geohash.substring(0, step);
				mapping.putIfAbsent(geohashKey, new ArrayList<>());
				mapping.get(geohashKey).add(hotel);
			}
		}
	}

	public MatchResult match(Geohashable geohashable, Range precision) {
		String geohash = geohashable.geohash(precision.getMax());
		MatchResult result = new MatchResult();
		if (geohash != null) {
			int max = precision.getMax() > geohash.length() ? geohash.length() : precision.getMax();
			int min = precision.getMin() > 0 ? precision.getMin() : 1;
			for (int step = max; step >= min; step--) {
				String geohashKey = geohash.substring(0, step);
				List<G> bucket = mapping.get(geohashKey);
				if (bucket != null && !bucket.isEmpty()) {
					G item = bucket.get(0);
					result = new MatchResult(item, step);
					break;
				}
			}
		}
		return result;
	}

	public boolean isEmpty() {
		return mapping.isEmpty();
	}

	public int size() {
		return (int) mapping.entrySet().stream().map(e -> e.getValue()).flatMap(List::stream).count();
	}

	public class MatchResult {

		private G item;
		private final Integer precision;

		public MatchResult() {
			this(null, null);
		}

		public MatchResult(G item, Integer precision) {
			this.item = item;
			this.precision = precision;
		}

		public G getItem() {
			return item;
		}

		public Integer getPrecision() {
			return precision;
		}

	}
}
