package anatoliisatanovskyi.bigdata201.spark.streaming.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface OutputWriter {

	void write(Dataset<Row> ds, String path);

	public static OutputWriter instanceOf(String type) {
		switch (type.toLowerCase()) {
		case "csv":
			return new CsvOutputWriter();
		case "es":
			return new ECWriter();
		default:
			throw new IllegalArgumentException("invalid output writer type: " + type);
		}
	}
}
