package anatoliisatanovskyi.bigdata201.orchestration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anatoliisatanovskyi.bigdata201.orchestration.dataset.DatasetProvider;
import anatoliisatanovskyi.bigdata201.orchestration.dataset.DatasetProviderImpl;
import anatoliisatanovskyi.bigdata201.orchestration.service.HotelStayService;
import anatoliisatanovskyi.bigdata201.orchestration.writer.OutputWriter;

public class Runner {

	private Config config = Config.instance();

	private DatasetProvider datasetProvider = new DatasetProviderImpl();
	private HotelStayService hotelStayService = new HotelStayService();
	private OutputWriter outputWriter;

	public Runner() {
		outputWriter = OutputWriter.instanceOf(config.getOutputType());
	}

	public static void main(String[] args) {

		try {
			Config.parse(args);
			Runner runner = new Runner();
			runner.execute();
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void execute() throws AnalysisException {
		SparkSession spark = createSparkSession();
		Dataset<Row> ds = datasetProvider.getDataset(spark);
		Dataset<Row> hotelStayDuration = hotelStayService.getHotelStayDuration(ds);
		outputWriter.write(hotelStayDuration, config.getOutputPath());
	}

	private SparkSession createSparkSession() {
		SparkConf sparkConf = createSparkConfig();
		return SparkSession.builder().config(sparkConf).getOrCreate();
	}

	private SparkConf createSparkConfig() {
		SparkConf sc = new SparkConf();
		sc.setAppName("Oozie spark transformation");
		sc.set("spark.master", "local[*]");
		if (config.getDefaultFS() != null) {
			sc.set("fs.defaultFS", config.getDefaultFS());
			sc.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		}
		return sc;
	}
}
