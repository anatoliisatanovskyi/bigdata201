package anatoliisatanovskyi.bigdata201.orchestration.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anatoliisatanovskyi.bigdata201.orchestration.Config;

public class DatasetProviderImpl implements DatasetProvider {

	private Config config = Config.instance();
	
	@Override
	public Dataset<Row> getDataset(SparkSession spark) {
		return spark.read().json(config.getInputPath());
	}

}
