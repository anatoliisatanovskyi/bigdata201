package anatoliisatanovskyi.bigdata201.spark.batching;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BatchingTest {

	@Test
	public void testExecuteRunner() throws Exception {
		Config config = Config.load(this.getClass().getClassLoader().getResource("config.properties").getFile());
		Runner runner = new Runner(config);
		runner.execute();
	}

}
