package anatoliisatanovskyi.bigdata201.orchestration.oozie.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anatoliisatanovskyi.bigdata201.orchestration.oozie.Config;

public class DatasetProviderImpl implements DatasetProvider {

	private Config config = Config.instance();
	
	@Override
	public Dataset<Row> getDataset(SparkSession spark) {
		return spark.read().json(config.getInputPath());
	}

}
