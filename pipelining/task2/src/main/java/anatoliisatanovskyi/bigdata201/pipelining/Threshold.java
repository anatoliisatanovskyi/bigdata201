package anatoliisatanovskyi.bigdata201.pipelining;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Used to enforce latency between http requests to prevent possible blacklisting
 */
public class Threshold {

	private static final long MIN_INTERVAL_MILLIS = 1000l;

	private long previousTimestamp;

	public void check() throws IOException {
		long diff = System.currentTimeMillis() - previousTimestamp;
		if (diff < MIN_INTERVAL_MILLIS) {
			try {
				ensureInterval(TimeUnit.MILLISECONDS, diff);
			} catch (InterruptedException e) {
				throw new IOException("interrupted", e);
			}
		}
		previousTimestamp = System.currentTimeMillis();
	}

	private void ensureInterval(TimeUnit unit, long interval) throws InterruptedException {
		unit.sleep(interval);
	}
}
