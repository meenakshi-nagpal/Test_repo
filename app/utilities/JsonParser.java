package utilities;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.logging.Logger;

// import models.storageapp.Metadata;

public class JsonParser {

	private static final Logger log = Logger.getLogger(JsonParser.class.getName());
	private static final ObjectMapper defaultObjectMapper = new ObjectMapper();

	// public static Metadata toObject(String filename) {
		
	// 	Metadata entry = new Metadata();
	// 	try {
	// 		JsonFactory jsonFactory = new JsonFactory();
	// 		com.fasterxml.jackson.core.JsonParser jp =
	// 		jsonFactory.createParser(new File(filename));
	// 		entry = new Metadata();

	// 		byte[] jsonData = Files.readAllBytes(Paths.get(filename));
	// 		ObjectMapper objectMapper = new ObjectMapper();
	// 		entry = objectMapper.readValue(jsonData, Metadata.class);
	// 	} catch (JsonParseException e) {
	// 		log.severe("JsonParseException In JsonParser.toObject(): " + e);
	// 	} catch (JsonMappingException e) {
	// 		log.severe("JsonMappingException In JsonParser.toObject(): " + e);
	// 	} catch (IOException e) {
	// 		log.severe("IOException In JsonParser.toObject(): " + e);
	// 	}
		
	// 	return entry;
	// }

	public static ObjectMapper mapper() {
		return defaultObjectMapper;
	}
	
	public static String toJson(Object data)
	{
		if(data == null){
			log.severe("Metadata object is empty to convert to JSon object. Failed at JsonParser.toJson().");
			return "";
		}

		try {
			return mapper().writeValueAsString(data);
		} catch (Exception e) {
			e.printStackTrace();
			log.severe("JsonProcessingException at JsonParser.toJson(). " + e);
			return "";
		}        
	}

	public static <T> T fromJson(String json, Class<T> clazz) {
		if (json == null || json.length() == 0) {
			log.severe("Json is blank");
			return null;
		}

		try {
			return mapper().readValue(json, clazz);
		} catch (Exception e) {
			e.printStackTrace();
			log.severe("JsonProcessingException at JsonParser.fromJson(). " + e);
			return null;
		} 		

	}
	
	// public Map<String, String> toMAp(String filename)
	// {
		
	// 	Map<String,String> jsonMap = new HashMap<String, String>();
		 
	// 	try {
	// 		byte[] mapData = Files.readAllBytes(Paths.get(filename));
	// 		ObjectMapper objectMapper = new ObjectMapper();
	// 		jsonMap = objectMapper.readValue(mapData, HashMap.class);
	// 	} catch (JsonParseException e) {
	// 		log.severe("JsonParseException at JsonParser.toMap()." + e);
	// 		return null;
	// 	} catch (JsonMappingException e) {
	// 		log.severe("JsonMappingException at JsonParser.toMap()." + e);
	// 		return null;
	// 	} catch (IOException e) {
	// 		log.severe("IOException at JsonParser.toMap()." + e);
	// 		return null;
	// 	}

	// 	return jsonMap;
		
	// }
}
