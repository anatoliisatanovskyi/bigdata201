package anatoliisatanovskyi.bigdata201.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import anatoliisatanovskyi.bigdata201.kafka.model.Weather;

public class WeatherSerializer implements Serializer<Weather> {
	@Override
	public void configure(Map<String, ?> map, boolean b) {
	}

	@Override
	public byte[] serialize(String arg0, Weather arg1) {
		byte[] retVal = null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			retVal = mapper.writeValueAsString(arg1).getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

	@Override
	public void close() {
	}
}
