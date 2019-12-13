package anatoliisatanovskyi.bigdata201.spark.batching.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper {

	private ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
			false);

	public <T> T fromJson(byte[] source, Class<T> c) throws JsonParseException, JsonMappingException, IOException {
		return mapper.readValue(source, c);
	}

	public <T> T fromJson(String source, Class<T> c) throws JsonParseException, JsonMappingException, IOException {
		return mapper.readValue(source, c);
	}
}
