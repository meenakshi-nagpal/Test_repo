package ws;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import models.storageapp.AppConfigProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import play.Logger;
import play.Play;
import play.libs.*;
import play.libs.WS.Response;
import play.libs.WS.WSRequestHolder;
import play.mvc.Http;
import utilities.Constant;

public class AIMSWSClient {

	public static JsonNode jerseyPostToAIMSAndGetJsonResponse(String url, JsonNode jsonNode) {
		try {
			String aimsUser = Play.application().configuration().getString("aims.user");
			String aimsPassword = Play.application().configuration().getString(
					"aims.password");
			Logger.debug("jerseyPostToAIMSAndGetJsonResponse - authenticating user "+aimsUser);
			
			Client client = ClientBuilder.newClient();
			HttpAuthenticationFeature feature = 
					HttpAuthenticationFeature.basic(aimsUser, aimsPassword);
			client.register(feature);

			WebTarget webTarget = client.target(url);

			ObjectMapper objectMapper = new ObjectMapper();
			final Object object = objectMapper.treeToValue(jsonNode, Object.class);
			final String json = objectMapper.writeValueAsString(object);
			//Logger.debug("jerseyPostToAIMSAndGetJsonResponse - Json string posted : "+json);
			
			javax.ws.rs.core.Response response = 
					webTarget.request().accept(MediaType.APPLICATION_JSON).post(
							Entity.entity(json , 
									MediaType.APPLICATION_JSON));

			if(response != null) {
				if(response.getStatus() == Http.Status.CREATED) {
					String responseString = response.readEntity(String.class);
					JsonNode jsonNode2 = objectMapper.readTree(responseString);
					return jsonNode2;
				} else {
					Logger.error("Service to " +  url + " returned result with " + 
							response.getStatus() + " - " + 
							response.getStatusInfo().getReasonPhrase());
				}
			}
		} catch (JsonProcessingException e) {
			Logger.error("AIMSWSClient - exception occurred while processing JSON", e);
			e.printStackTrace();
		} catch (IOException e) {
			Logger.error("AIMSWSClient - exception occurred while calling AIMS Service", e);
			e.printStackTrace();
		}

		return null;
	}
	
	public static int jerseyPostToAIMSAndGetStatus(String url, JsonNode jsonNode) {
		try {
			String aimsUser = Play.application().configuration().getString("aims.user");
			String aimsPassword = Play.application().configuration().getString(
					"aims.password");
			Logger.debug("jerseyPostToAIMSAndGetStatus - authenticating user "+aimsUser);
			
			Client client = ClientBuilder.newClient();
			HttpAuthenticationFeature feature = 
					HttpAuthenticationFeature.basic(aimsUser, aimsPassword);
			client.register(feature);

			WebTarget webTarget = client.target(url);

			ObjectMapper objectMapper = new ObjectMapper();
			final Object object = objectMapper.treeToValue(jsonNode, Object.class);
			final String json = objectMapper.writeValueAsString(object);
			
			javax.ws.rs.core.Response response = 
					webTarget.request().accept(MediaType.APPLICATION_JSON).post(
							Entity.entity(json , 
									MediaType.APPLICATION_JSON));

			if(response != null) {
				return response.getStatus();
			}
			else {
				Logger.error("Service to " +  url + " returned result with null response");
				return -1;
			}
		} catch (JsonProcessingException e) {
			Logger.error("AIMSWSClient - exception occurred while processing JSON", e);
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			Logger.error("AIMSWSClient - exception occurred while calling AIMS Service", e);
			e.printStackTrace();
			return -1;
		}

	}
	

	public static JsonNode jerseyGetAIMSJsonResponse(String url) {
		try {
			String aimsUser = Play.application().configuration().getString("aims.user");
			String aimsPassword = Play.application().configuration().getString(
					"aims.password");
			Logger.debug("jerseyGetAIMSJsonResponse - authenticating user "+aimsUser);
			
			Client client = ClientBuilder.newClient();
			HttpAuthenticationFeature feature = 
					HttpAuthenticationFeature.basic(aimsUser, aimsPassword);
			client.register(feature);

			WebTarget webTarget = client.target(url);
			
			javax.ws.rs.core.Response response = webTarget.request().accept(MediaType.APPLICATION_JSON).get();

			if(response != null) {
				if(response.getStatus() == Http.Status.OK) {
					ObjectMapper objectMapper = new ObjectMapper();
					String responseString = response.readEntity(String.class);
					JsonNode jsonNode = objectMapper.readTree(responseString);
					//Logger.debug("jerseyGetAIMSJsonResponse - Response for the get req : "+jsonNode.toString());
					return jsonNode;
				} else {
					Logger.error("jerseyGetAIMSJsonResponse Service to " +  url + " returned result with " + 
							response.getStatus() + " - " + 
							response.getStatusInfo().getReasonPhrase());
				}
			}
		} catch (JsonProcessingException e) {
			Logger.error("AIMSWSClient - exception occurred while processing JSON", e);
		} catch (IOException e) {
			Logger.error("AIMSWSClient - exception occurred while calling AIMS Service", e);
			e.printStackTrace();
		}
		return null;
	}
	
	public static JsonNode jerseyPutToAIMSAndGetJsonResponse(String url, JsonNode jsonNode) {
		try {
			String aimsUser = Play.application().configuration().getString("aims.user");
			String aimsPassword = Play.application().configuration().getString(
					"aims.password");
			Logger.debug("jerseyPostToAIMSAndGetJsonResponse - authenticating user "+aimsUser);
			
			Client client = ClientBuilder.newClient();
			HttpAuthenticationFeature feature = 
					HttpAuthenticationFeature.basic(aimsUser, aimsPassword);
			client.register(feature);

			WebTarget webTarget = client.target(url);

			ObjectMapper objectMapper = new ObjectMapper();
			final Object object = objectMapper.treeToValue(jsonNode, Object.class);
			final String json = objectMapper.writeValueAsString(object);
			Logger.debug("jerseyPutToAIMSAndGetJsonResponse - Json string posted : "+json);
			
			javax.ws.rs.core.Response response = 
					webTarget.request().accept(MediaType.APPLICATION_JSON).put(
							Entity.entity(json , 
									MediaType.APPLICATION_JSON));

			if(response != null) {
				Logger.debug("jerseyPutToAIMSAndGetJsonResponse - response status "+response.getStatus());
				if(response.getStatus() == Http.Status.OK || response.getStatus() == 200) {
					String responseString = response.readEntity(String.class);
					JsonNode jsonNode2 = objectMapper.readTree(responseString);
					return jsonNode2;
				} else {
					Logger.error("jerseyPostToAIMSAndGetJsonResponse Service to " +  url + " returned result with " + 
							response.getStatus() + " - " + 
							response.getStatusInfo().getReasonPhrase());
				}
			}
		} catch (JsonProcessingException e) {
			Logger.error("AIMSWSClient - exception occurred while processing JSON", e);
			e.printStackTrace();
		} catch (IOException e) {
			Logger.error("AIMSWSClient - exception occurred while calling AIMS Service", e);
			e.printStackTrace();
		}

		return null;
	}
	
	public static JsonNode postToAIMSAndGetJsonResponse(String url, JsonNode jsonNode) {
		WSRequestHolder requestHolder =  WS.url(url);
		setAIMSCredentials(requestHolder);

		Response response = getResponse(requestHolder.post(jsonNode));

		if(response != null) {
			if(response.getStatus() == Http.Status.CREATED) {
				return response.asJson();
			}	
			else
			{
				Logger.error("Service to " +  url + " returned result with " + response.getStatus() + " - " + response.getStatusText());
			}
		}

		return null;
	}

	public static JsonNode getAIMSJsonResponse(String url) {
		WSRequestHolder requestHolder =  WS.url(url);
		setAIMSCredentials(requestHolder);

		Response response = getResponse(requestHolder.get());
		
		if(response != null) {
			if(response.getStatus() == Http.Status.OK) {
				return response.asJson();
			}		
		}

		return null;
	}


	public static Response postToAIMSAndGetResponse(String url, String data) {
		WSRequestHolder requestHolder =  WS.url(url);
		setAIMSCredentials(requestHolder);

		Response response = getResponse(requestHolder.post(data));

		if(response != null) {
			if(response.getStatus() == Http.Status.CREATED) {
				return response;
			}		
		}

		return null;
	}

	public static Response postToAIMSAndGetResponse(String url, 
			InputStream inputStream) {
		WSRequestHolder requestHolder =  WS.url(url);
		setAIMSCredentials(requestHolder);

		Response response = getResponse(requestHolder.post(inputStream));

		if(response != null) {
			if(response.getStatus() == Http.Status.CREATED) {
				return response;
			}		
		}
		return null;
	}

	private static void setAIMSCredentials(WSRequestHolder requestHolder) {
		String aimsUser = Play.application().configuration().getString(Constant.AIMS_USER);
		String aimsPassword = Play.application().configuration().getString(
				Constant.AIMS_PASSWORD);

		if(aimsUser == null || aimsUser.trim().isEmpty()) {
			throw new IllegalArgumentException(Constant.AIMS_USER + " property not set.");
		}

		if(aimsPassword == null || aimsPassword.trim().isEmpty()) {
			throw new IllegalArgumentException(Constant.AIMS_PASSWORD + " property not set.");
		}

		requestHolder.setAuth(aimsUser, aimsPassword);

	}

	private static Response getResponse(F.Promise<Response> promise) {
		long aimsTimeoutInSeconds = Constant.DEFAULT_AIMS_WS_TIMEOUT_SECONDS;

		AppConfigProperty appConfigProperty = 
				AppConfigProperty.getPropertyByKey(Constant.AIMS_WS_TIMEOUT_SECONDS_KEY);

		if(appConfigProperty != null && 
				appConfigProperty.getLongValue() != null) {
			aimsTimeoutInSeconds = appConfigProperty.getLongValue();
		}

		return promise.get(aimsTimeoutInSeconds, 
				TimeUnit.SECONDS);

	}

}
