package anatoliisatanovskyi.bigdata201.orchestration;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroWriterTest {

	private static final String INPUT_PATH = AvroWriterTest.class.getClassLoader().getResource("hotelWeather.json")
			.getFile();
	private static final String OUTPUT_PATH = "result.avro";

	@BeforeClass
	public static void beforeEach() {
		Config.parse("--inputPath=" + INPUT_PATH, //
				"--outputPath=" + OUTPUT_PATH, //
				"--outputType=avro");
	}

	@AfterClass
	public static void afterEach() throws IOException {
		File avro = new File(OUTPUT_PATH);
		if (avro.exists()) {
			FileUtils.deleteDirectory(avro);
		}
	}

	@Test
	public void testWriteAvro() throws Exception {

		Runner runner = new Runner();
		runner.execute();

		File avro = new File(OUTPUT_PATH);
		assertTrue(avro.exists());
	}
}
