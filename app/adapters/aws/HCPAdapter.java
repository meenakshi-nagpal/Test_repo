package adapters.aws;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import utilities.Constant;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import java.util.logging.Logger;
import adapters.AdapterException;
import adapters.cas.CenteraClip;
import java.io.StringWriter;
import java.io.PrintWriter;

public class HCPAdapter{
	
	private static final Logger log = Logger.getLogger(HCPAdapter.class.getName());
	
	
	private String dateFormat = "yyyy.MM.dd HH:mm:ss z";
	
	private String hcpEndUrl;
	private String bucketName;
	private String accessKey;
	private String secretKey;
	private String hcpKey="HCP {accessKey}:{SecretKey}";
	
	//Constructor
	public HCPAdapter(String hcpEndUrl,String acckey,String secKey){
		
		if(hcpEndUrl==null || acckey==null || secKey==null  || hcpEndUrl.isEmpty() || acckey.isEmpty()  || secKey.isEmpty()){
			throw new IllegalArgumentException("HCP End URL or AccessKey or secretKey cannot be null or empty");
		}
		
		this.hcpEndUrl=hcpEndUrl;   
		this.accessKey=acckey;
		this.secretKey=secKey;
	}
	

	//tp get HCP format access and secret ket
     public String getEndKey(){
	     
		 String endKey=hcpKey.replace(Constant.HCP_URL_ACCESS_KEY, accessKey);
		 return endKey.replace(Constant.HCP_URL_SECRET_KEY, secretKey);
	 }
	
	 public int applyRetentionEndDate(String fileName, Long retentionEndDate)throws AdapterException{
		try{
			
		 String endURL=hcpEndUrl+fileName+"?retention="+retentionEndDate;
		 HttpClient client = HttpClientBuilder.create().build();
		 HttpPost request = new HttpPost(endURL);
	    
		 //add authorization header for user(base64) "exampleuser" with password(md5) "passw0rd"
		 request.addHeader("Authorization", getEndKey());
		 //print response status to console
		 log.info("HCPAdapter:Applying retention- "+fileName);
		 HttpResponse response = client.execute(request);
		 log.info("Response Code : " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
		 
		 if(response.getStatusLine().getStatusCode()==200){
			 return 1;
		 }else{
			 throw new AdapterException("HCP ADAPTER: Could not able to apply retention. Guid-"+fileName);
		 }
			
		}catch (AdapterException e) {
			log.severe("HCP ADAPTER: Could not able to apply retention "+fileName+" "+ e.getMessage());
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			log.severe("HCP ADAPTER: Error while applying retention " +stackTraceAsString);
			throw new AdapterException(e);
		}
		catch(Exception e) {
			log.severe("HCP ADAPTER: Exception occured while applying Retention End date, fileName:"+fileName+" "+e.getMessage());
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			log.severe("HCP ADAPTER: Exception occured while applying Retention End date"+stackTraceAsString);
		
			throw new AdapterException(e);
		}	
		
	 }
	
	public int delete(String GUID) throws IOException{
		try{
		
		HttpClient client = HttpClientBuilder.create().build();
		HttpDelete request = new HttpDelete(hcpEndUrl);
		log.info("delete url-"+hcpEndUrl);
		request.addHeader("Authorization", getEndKey());
		//execute DELETE request
		HttpResponse response = client.execute(request);
		//print response status to console
		log.info("Response Code : " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
		
		}catch(Exception e) {
			log.severe("Exception occured while deleting archive :"+e.getMessage());
			log.severe("HCPAdapter: Exception occured while deleting archive: "+e);
			return -1;
			}	
		 return 1;
		}
	
	public int privdelete(String filename,String reason) throws IOException{
		try{
			
		String url=hcpEndUrl+filename+"?privileged=true&reason="+reason;
		log.info("HCPAdapter:privdelete filename-"+filename);
		//create a new HttpClient object and a DELETE request object
		HttpClient client = HttpClientBuilder.create().build();
		HttpDelete request = new HttpDelete(url);
		
		 // add authorization header for user(base64) "exampleuser" with password(md5) "passw0rd"
	     request.addHeader("Authorization", getEndKey());
		 //execute DELETE request
	     HttpResponse response = client.execute(request);
		 int status=0;
		 if(null != response)
		    status = response.getStatusLine().getStatusCode();

		 if(status==200){
			 return 1;
		 }
 
		}
		  catch(Exception e) {
			log.severe("HCPAdapter: Error occured while issuing Priv delete, filename:"+filename+" "+e.getMessage());
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			
			log.severe("HCPAdapter: Exception occured while priv delete archive: "+stackTraceAsString);
			return -1;
		}
		
		 return -1;
	}
}
