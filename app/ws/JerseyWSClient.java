package ws;

import java.io.InputStream;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ChunkParser;
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import play.Logger;
import play.Play;
import utilities.Constant;

public class JerseyWSClient {
	
	
	public static ChunkedInput<String> postToAIMSAndGetChunkedResponse(String url, 
			InputStream inputStream, String chunkParseString) {
		String aimsUser = Play.application().configuration().getString(Constant.AIMS_USER);
		String aimsPassword = Play.application().configuration().getString(
				Constant.AIMS_PASSWORD);

		Client client = ClientBuilder.newClient();
		HttpAuthenticationFeature feature = 
				HttpAuthenticationFeature.basic(aimsUser, aimsPassword);
		client.register(feature);

		WebTarget webTarget = client.target(url);
		
		Response response = 
				webTarget.request().post(
						Entity.entity(
								inputStream, 
								MediaType.APPLICATION_OCTET_STREAM_TYPE));
		Logger.debug("postToAIMSAndGetChunkedResponse: Response Object: " + response);

		final ChunkedInput<String> chunkedInput = 
				response.readEntity(
						new GenericType<ChunkedInput<String>>() {}); 
		
		ChunkParser newChunkParser =  ChunkedInput.createParser(chunkParseString);
		chunkedInput.setParser(newChunkParser);
		Logger.debug("postToAIMSAndGetChunkedResponse: ChunkedInput Object: " +  
				chunkedInput);
		
		return chunkedInput;
	}
}
