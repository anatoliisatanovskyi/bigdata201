package anatoliisatanovskyi.bigdata201.kafka;

public class Range {

	private final int min;
	private final int max;

	private Range(int min, int max) {
		this.min = min;
		this.max = max;
	}

	public static Range of(int min, int max) {
		if (min > max) {
			throw new IllegalArgumentException(String.format("illelgal range, min(%d) greater then max(%d)", min, max));
		}
		return new Range(min, max);
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}
}
