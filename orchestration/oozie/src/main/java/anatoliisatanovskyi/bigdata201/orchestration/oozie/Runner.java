package anatoliisatanovskyi.bigdata201.orchestration.oozie;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anatoliisatanovskyi.bigdata201.orchestration.oozie.dataset.DatasetProvider;
import anatoliisatanovskyi.bigdata201.orchestration.oozie.dataset.DatasetProviderImpl;
import anatoliisatanovskyi.bigdata201.orchestration.oozie.service.HotelStayService;
import anatoliisatanovskyi.bigdata201.orchestration.oozie.writer.CsvOutputWriter;
import anatoliisatanovskyi.bigdata201.orchestration.oozie.writer.OutputWriter;

public class Runner {

	private Config config = Config.instance();

	private DatasetProvider datasetProvider = new DatasetProviderImpl();
	private HotelStayService hotelStayService = new HotelStayService();
	private OutputWriter outputWriter = new CsvOutputWriter();

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
		return new SparkConf().setAppName("Oozie spark transformation")//
				.set("fs.defaultFS", config.getDefaultFS())//
				.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")//
				.set("spark.master", "local[*]");
	}
}
