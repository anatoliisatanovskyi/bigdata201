package anatoliisatanovskyi.bigdata201.kafka;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ProcessorTest {

	@Test
	public void testName() throws Exception {
		String configFilepath = "C:\\sources\\bigdata201\\kafka\\kafka-task2\\src\\test\\resources\\config.properties";
		HotelsWeatherProcessor.loadConfiguration(configFilepath);
		HotelsWeatherProcessor processor = new HotelsWeatherProcessor();
		processor.readAndMapHotelsData();
		processor.consumeEnrichAndProduceData();
	}

	@Test
	public void testName2() throws Exception {
		String configFilepath = "C:\\sources\\bigdata201\\kafka\\kafka-task2\\src\\test\\resources\\config.properties";
		HotelsWeatherProcessor.loadConfiguration(configFilepath);
		HotelsWeatherProcessor processor = new HotelsWeatherProcessor();
		System.out.println(processor.readHotelsDataFromKafka().size());
	}
}
