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
			for (int step = precision.getMin(); step <= geohash.length() || step <= precision.getMax(); step++) {
				String geohashKey = geohash.substring(0, step);
				mapping.putIfAbsent(geohashKey, new ArrayList<>());
				mapping.get(geohashKey).add(hotel);
			}
		}
	}

	public Result match(Geohashable geohashable, Range precision) {
		String geohash = geohashable.geohash(precision.getMax());
		Result result = new Result();
		if (geohash != null) {
			for (int step = precision.getMax(); step >= geohash.length() && step >= precision.getMin(); step--) {
				String geohashKey = geohash.substring(0, step);
				List<G> bucket = mapping.get(geohashKey);
				if (bucket != null && !bucket.isEmpty()) {
					G item = bucket.get(0);
					result = new Result(item, step);
					break;
				}
			}
		}
		return result;
	}

	public int size() {
		return mapping.size();
	}

	public class Result {

		private G item;
		private final Integer precision;

		public Result() {
			this(null, null);
		}

		public Result(G item, Integer precision) {
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
