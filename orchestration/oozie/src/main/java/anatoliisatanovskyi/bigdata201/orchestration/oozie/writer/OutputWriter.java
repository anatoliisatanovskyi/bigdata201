package anatoliisatanovskyi.bigdata201.orchestration.oozie.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface OutputWriter {

	void write(Dataset<Row> ds, String path);

}
