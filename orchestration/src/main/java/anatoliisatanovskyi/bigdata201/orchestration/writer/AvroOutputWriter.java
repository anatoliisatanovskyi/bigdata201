package anatoliisatanovskyi.bigdata201.orchestration.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class AvroOutputWriter implements OutputWriter {

	@Override
	public void write(Dataset<Row> ds, String path) {
		ds.write().format("com.databricks.spark.avro").save(path);
	}

}
