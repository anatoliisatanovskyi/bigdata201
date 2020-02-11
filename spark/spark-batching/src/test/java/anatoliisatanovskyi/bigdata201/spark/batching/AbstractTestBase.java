package anatoliisatanovskyi.bigdata201.spark.batching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.BeforeClass;

import anatoliisatanovskyi.bigdata201.spark.batching.dataset.DatasetProvider;
import anatoliisatanovskyi.bigdata201.spark.batching.dataset.TestDatasetProvider;

public class AbstractTestBase {

	private Optional<SparkConf> sparkConf = Optional.empty();
	private Optional<JavaSparkContext> javaSparkContext = Optional.empty();
	private Optional<SparkSession> sparkSession = Optional.empty();

	private DatasetProvider datasetProvider = new TestDatasetProvider();

	private static Config config;

	@BeforeClass
	public static void beforeClass() throws FileNotFoundException, IOException {
		if (config == null) {
			config = Config.load(DatasetProviderTest.class.getClassLoader().getResource("config.properties").getFile());
		}
	}

	@After
	public void stopSparkContext() {
		sparkSession.ifPresent(c -> c.close());
	}

	protected SparkSession spark() {
		if (!sparkSession.isPresent()) {
			sparkSession = Optional.of(createSparkSession());
		}
		return sparkSession.get();
	}

	protected JavaSparkContext jsc() {
		if (!javaSparkContext.isPresent()) {
			javaSparkContext = Optional.of(createJavaSparkContext());
		}
		return javaSparkContext.get();
	}

	protected SparkConf conf() {
		if (!sparkConf.isPresent()) {
			sparkConf = Optional.of(createConf());
		}
		return sparkConf.get();
	}

	protected SparkConf createConf() {
		return new SparkConf().setMaster("local[*]").setAppName(getClass().getSimpleName()).set("spark.ui.enabled",
				"false");
	}

	protected SparkSession createSparkSession() {
		return SparkSession.builder().config(conf()).getOrCreate();
	}

	protected JavaSparkContext createJavaSparkContext() {
		return new JavaSparkContext(spark().sparkContext());
	}

	protected DatasetProvider dsp() {
		return datasetProvider;
	}
}
