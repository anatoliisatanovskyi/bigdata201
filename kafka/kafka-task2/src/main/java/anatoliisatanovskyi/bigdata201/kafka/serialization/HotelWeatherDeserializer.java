package anatoliisatanovskyi.bigdata201.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import anatoliisatanovskyi.bigdata201.kafka.model.HotelWeather;

public class HotelWeatherDeserializer implements Deserializer<HotelWeather> {

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public HotelWeather deserialize(String arg0, byte[] arg1) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		HotelWeather user = null;
		try {
			user = mapper.readValue(arg1, HotelWeather.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return user;
	}

	@Override
	public void close() {
	}
}