package utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;




public class ApacheClientUtil {
	private static final Logger log = Logger.getLogger(ApacheClientUtil.class.getName());

	public static String get(String url) {
		return doGet(url, null);
	}

	public static String privateGet(String url, String username, String password) {
		return doGet(url, "ILM " + username + ":" + password);
	}
	

	/***
	* Return HTTP response body as a String; not suitable for big size response body 
	*/
	private static String doGet(String url, String authorization) {

		String certificateFile = System.getProperty("https.keyStore");
		if (certificateFile == null || certificateFile.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}
		String certPass = System.getProperty("https.keyStorePassword");
		if (certPass == null || certPass.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}		

		boolean isAuthorized = false;
		CloseableHttpClient client = null;
		CloseableHttpResponse  response = null;
		Header xbacHeader = null;
		FileInputStream instream = null;
		KeyStore trustStore = null;
		try {
			trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

			instream = new FileInputStream(new File(certificateFile));

			trustStore.load(instream, certPass.toCharArray());

			// Trust own CA and all self-signed certs
			SSLContext sslcontext = SSLContexts
					.custom()
					.loadTrustMaterial(trustStore,
							new TrustSelfSignedStrategy()).build();
			// Allow TLSv1 protocol only
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
					sslcontext,
					new String[] {"TLSv1"},
					new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA"},
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			client = HttpClients.custom()
					.setSSLSocketFactory(sslsf).build();

			HttpUriRequest request = RequestBuilder.get().setUri(url)
					.setHeader(HttpHeaders.ACCEPT, "application/json")
					.build();
			
			if (authorization != null && authorization.length() != 0) {
				request.addHeader("Authorization", authorization);
			}
			
			response = client.execute(request);
			if (response != null) {
				HttpEntity httpEntity = response.getEntity();
				String entity = EntityUtils.toString(httpEntity);
				EntityUtils.consume(httpEntity);
				return entity;
			}
		} 
		catch(IOException ioe)
		{
			log.severe("IOException In checkRBacResponse(): " + ioe);
		}
		catch(NoSuchAlgorithmException nsae)
		{
			log.severe("NoSuchAlgorithmException In checkRBacResponse(): " + nsae);
		}
		catch(KeyStoreException kse)
		{
			log.severe("KeyStoreException In checkRBacResponse(): " + kse);
		}
		catch(KeyManagementException kme)
		{
			log.severe("KeyManagementException In checkRBacResponse(): " + kme);
		}
		catch(CertificateException ce)
		{
			log.severe("CertificateException In checkRBacResponse(): " + ce);
		}
		finally {
			try {
				if (client != null) client.close();
				if (response != null) response.close();
				if (instream != null) instream.close();
			} catch (IOException e) {
				log.severe("IOException In checkRBacResponse(): " + e);
			}
		}
		return "";
	}
	
	public static String updateArchiveMetaDataSSL(String url, String UpdatedClipAsJson, String user, String password) throws Exception {
		String certificateFile = System.getProperty("https.keyStore");
		if (certificateFile == null || certificateFile.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}
		String certPass = System.getProperty("https.keyStorePassword");
		if (certPass == null || certPass.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}	
		
		
		CloseableHttpClient client = null;
		CloseableHttpResponse  response = null;
		Header xbacHeader = null;
		FileInputStream instream = null;
		KeyStore trustStore = null;
		
		StringBuilder output = new StringBuilder();
		String line ="";
		
		try{
			trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			instream = new FileInputStream(new File(certificateFile));
			trustStore.load(instream, certPass.toCharArray());
	
			// Trust own CA and all self-signed certs
			SSLContext sslcontext = SSLContexts
					.custom()
					.loadTrustMaterial(trustStore,
							new TrustSelfSignedStrategy()).build();
			// Allow TLSv1 protocol only
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
					sslcontext,
					new String[] {"TLSv1"},
					new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA"},
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
	
			client = HttpClients.custom()
					.setSSLSocketFactory(sslsf).build();
	
	
			HttpPut put = new HttpPut(url);
			if(!"".equals(UpdatedClipAsJson) && UpdatedClipAsJson != null){
				StringEntity input =new StringEntity(UpdatedClipAsJson);
				input.setContentType("application/json");
				put.setEntity(input);
			}
			put.addHeader("Authorization", "ILM " + user + ":" + password);
			
			response = client.execute(put);
			
			BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

			while ((line = br.readLine()) != null) {
				output.append(line);
			}
 
		}catch(ClientProtocolException e){
			log.severe("ClientProtocolException In persistArchiveMetaData()" + e);
			e.printStackTrace();
			throw e;
		}
		catch(IOException ioe)
		{
			log.severe("IOException In checkRBacResponse(): " + ioe);
		}
		catch(NoSuchAlgorithmException nsae)
		{
			log.severe("NoSuchAlgorithmException In checkRBacResponse(): " + nsae);
		}
		catch(KeyStoreException kse)
		{
			log.severe("KeyStoreException In checkRBacResponse(): " + kse);
		}
		catch(KeyManagementException kme)
		{
			log.severe("KeyManagementException In checkRBacResponse(): " + kme);
		}
		catch(CertificateException ce)
		{
			log.severe("CertificateException In checkRBacResponse(): " + ce);
		}
		catch(Exception e){
			 throw e;	
		}
		finally {
			try {
				if (client != null) client.close();
				if (response != null) response.close();
				if (instream != null) instream.close();
			} catch (IOException e) {
				log.severe("IOException In checkRBacResponse(): " + e);
			}
		}

		return output.toString();
	}
	
	public static String persistArchiveMetaDataSSL(String url, String clipAsJson, String user, String password) throws Exception {
		
		String certificateFile = System.getProperty("https.keyStore");
		if (certificateFile == null || certificateFile.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}
		String certPass = System.getProperty("https.keyStorePassword");
		if (certPass == null || certPass.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}	
		
		
		CloseableHttpClient client = null;
		CloseableHttpResponse  response = null;
		Header xbacHeader = null;
		FileInputStream instream = null;
		KeyStore trustStore = null;
		
		StringBuilder output = new StringBuilder();
		String line ="";
		
		try{
			trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			instream = new FileInputStream(new File(certificateFile));
			trustStore.load(instream, certPass.toCharArray());
	
			// Trust own CA and all self-signed certs
			SSLContext sslcontext = SSLContexts
					.custom()
					.loadTrustMaterial(trustStore,
							new TrustSelfSignedStrategy()).build();
			// Allow TLSv1 protocol only
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
					sslcontext,
					new String[] {"TLSv1"},
					new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA"},
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
	
			client = HttpClients.custom()
					.setSSLSocketFactory(sslsf).build();
	
	
			HttpPut put = new HttpPut(url);
		
			StringEntity input =new StringEntity(clipAsJson);
			input.setContentType("application/json");
			
			put.addHeader("Authorization", "ILM " + user + ":" + password);
			put.setEntity(input);
			
			response = client.execute(put);
			
			BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

			while ((line = br.readLine()) != null) {
				output.append(line);
			}
 
		}catch(ClientProtocolException e){
			log.severe("ClientProtocolException In persistArchiveMetaData()" + e);
			e.printStackTrace();
			throw e;
		}
		catch(IOException ioe)
		{
			log.severe("IOException In checkRBacResponse(): " + ioe);
		}
		catch(NoSuchAlgorithmException nsae)
		{
			log.severe("NoSuchAlgorithmException In checkRBacResponse(): " + nsae);
		}
		catch(KeyStoreException kse)
		{
			log.severe("KeyStoreException In checkRBacResponse(): " + kse);
		}
		catch(KeyManagementException kme)
		{
			log.severe("KeyManagementException In checkRBacResponse(): " + kme);
		}
		catch(CertificateException ce)
		{
			log.severe("CertificateException In checkRBacResponse(): " + ce);
		}
		catch(Exception e){
			 throw e;	
		}
		finally {
			try {
				if (client != null) client.close();
				if (response != null) response.close();
				if (instream != null) instream.close();
			} catch (IOException e) {
				log.severe("IOException In checkRBacResponse(): " + e);
			}
		}

		return output.toString();
	}
	
	/***
	 * This method is used for creating the AccessData in the stream app for both cases - With Access and Without Access
	 * @param accessDataUrl
	 * @param accessDataAsJson
	 */
	public static String persistAccessData(String accessDataUrl, String accessDataAsJson, String user ) throws Exception {
		
		String certificateFile = System.getProperty("https.keyStore");
		if (certificateFile == null || certificateFile.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}
		String certPass = System.getProperty("https.keyStorePassword");
		if (certPass == null || certPass.length() ==0) {
			log.severe("key store file path or key pass is blank");
			return "";
		}	
		
		
		final String password =System.getenv("STREAM_SIG");
		if (password == null || password.length() ==0) {
			log.severe("STREAM_SIG environment variable is blank.");			
    		return "";
		}
		
		
		CloseableHttpClient client = null;
		CloseableHttpResponse  response = null;
		FileInputStream instream = null;
		KeyStore trustStore = null;
		StringBuilder output = new StringBuilder();
		String line ="";
		
		
		try{
			trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			instream = new FileInputStream(new File(certificateFile));
			trustStore.load(instream, certPass.toCharArray());
	
			// Trust own CA and all self-signed certs
			SSLContext sslcontext = SSLContexts
					.custom()
					.loadTrustMaterial(trustStore,
							new TrustSelfSignedStrategy()).build();
			// Allow TLSv1 protocol only
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
					sslcontext,
					new String[] {"TLSv1"},
					new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA"},
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
	
			client = HttpClients.custom().setSSLSocketFactory(sslsf).build();
			
	
			HttpPut put = new HttpPut(accessDataUrl);				
			StringEntity input =new StringEntity(accessDataAsJson);
			input.setContentType("application/json");
			put.addHeader("Authorization", "ILM " + user + ":" + password);
			put.setEntity(input);
			
			response = client.execute(put);		
			BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

			while ((line = br.readLine()) != null) {
				output.append(line);
			}
			
 
		}catch(ClientProtocolException e){
			log.severe("ClientProtocolException In persistAccessData()" + e);
			e.printStackTrace();
			throw e;
		}
		catch(IOException ioe)
		{
			log.severe("IOException In persistAccessData(): " + ioe);
		}
		catch(NoSuchAlgorithmException nsae)
		{
			log.severe("NoSuchAlgorithmException In persistAccessData(): " + nsae);
		}
		catch(KeyStoreException kse)
		{
			log.severe("KeyStoreException In persistAccessData(): " + kse);
		}
		catch(KeyManagementException kme)
		{
			log.severe("KeyManagementException In persistAccessData(): " + kme);
		}
		catch(CertificateException ce)
		{
			log.severe("CertificateException In persistAccessData(): " + ce);
		}
		catch(Exception e){
			 throw e;	
		}
		finally {
			try {
				if (client != null) client.close();
				if (response != null) response.close();
				if (instream != null) instream.close();
			} catch (IOException e) {
				log.severe("IOException In persistAccessData(): " + e);
			}
		}
		return output.toString();
		
	}
	
}