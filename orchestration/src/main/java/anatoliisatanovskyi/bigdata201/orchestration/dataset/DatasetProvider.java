package anatoliisatanovskyi.bigdata201.orchestration.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DatasetProvider {

	Dataset<Row> getDataset(SparkSession spark);
	
}
