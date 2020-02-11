package anatoliisatanovskyi.bigdata201.orchestration.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvOutputWriter implements OutputWriter {

	@Override
	public void write(Dataset<Row> ds, String path) {
		ds.repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save(path);
	}

}
