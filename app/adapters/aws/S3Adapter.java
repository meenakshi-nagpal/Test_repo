package adapters.aws;

//s3
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

import java.util.logging.Logger;

import utilities.Constant;
import adapters.cas.CenteraClip;
import adapters.AdapterException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;
import compression.CompressInputStream;
import compression.CompressionUtility;



import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class S3Adapter{
	
	private static final Logger log = Logger.getLogger(S3Adapter.class.getName());
	
	private String s3EndUrl;
	private String bucketName;
	private String accessKey;
	private String secretKey;
	
	
	
	public S3Adapter(String s3EndUrl, String bucketName, String accessKey,String secretKey) {

		if(s3EndUrl == null || bucketName == null || accessKey == null || secretKey==null ||
				s3EndUrl.trim().isEmpty() || bucketName.trim().isEmpty() ||	accessKey.trim().isEmpty() || secretKey.trim().isEmpty()) {
			throw new IllegalArgumentException("s3EndUrl or bucketName or accessKey or secretKey cannot be null or empty");
		}
		
		this.bucketName = bucketName;
		this.s3EndUrl = s3EndUrl;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}
	
	public AmazonS3  getS3connection() {
		
		AmazonS3 s3=null;
		try{
		
		// ClientConfiguration clientConfig = new ClientConfiguration();
	    // clientConfig.setProtocol(Protocol.HTTPS);
		log.info("S3Adapter:getS3connection s3EndUrl "+s3EndUrl);
		AWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
     	s3 = new AmazonS3Client(credentials);   	
	 	s3.setEndpoint(s3EndUrl);	
	 	s3.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
	
		return s3;
		
		} catch (AmazonServiceException ase) {
			log.severe("Caught an AmazonServiceException, which means your request made it "
	                + "to Amazon S3, but was rejected with an error response for some reason.");
			log.severe("Error Message:    " + ase.getMessage());
			log.severe("HTTP Status Code: " + ase.getStatusCode());
			log.severe("AWS Error Code:   " + ase.getErrorCode());
			log.severe("Error Type:       " + ase.getErrorType());
			log.severe("Request ID:       " + ase.getRequestId());
			
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			ase.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			log.severe("S3Adapter:Error in AmazonServiceException "+stackTraceAsString);
	       
	        return null;
	    } catch (AmazonClientException ace) {
	    	log.severe("Caught an AmazonClientException, which means the client encountered "
	                + "a serious internal problem while trying to communicate with S3, "
	                + "such as not being able to access the network.");
	    	StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			ace.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
	        log.severe("Error Message: " + ace.getMessage());
	        log.severe("S3Adapter:Error in AmazonClientException: " +stackTraceAsString);
	        return null;
	      
	    }catch (Exception e) {
	    	log.severe("S3Adapter: Amazon S3 Error Message: " + e.getMessage());
	    	StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
	    	log.severe("S3Adapter: Error in creating S3 Connection: "+stackTraceAsString);
			
		    return null;
		}
		
	}
	
	public CenteraClip upload(CenteraClip centeraClip, String fileName, long contentLength) throws AdapterException {
		
		InputStream compressedInput=null;
		ByteArrayInputStream byteArrayInputStream  =null;
		
		try {
		AmazonS3 s3=getS3connection();
		if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");
			
		log.info("S3Adapter: Starting uploading file: "+fileName);
		if(centeraClip.getCompressionEnabled().equalsIgnoreCase("true")){ 	
			compressedInput = new CompressInputStream(centeraClip.getInputStream());
			byte[] bytes = getByteByInputStream(compressedInput);
			ObjectMetadata meta = new ObjectMetadata();
			byteArrayInputStream = new ByteArrayInputStream(bytes);
			meta.setContentLength(bytes.length);
			s3.putObject(new PutObjectRequest(bucketName, fileName, byteArrayInputStream,meta));
			centeraClip.size = bytes.length; 
			return centeraClip;
		}else{
			ObjectMetadata meta = new ObjectMetadata();
			meta.setContentLength(contentLength);
			s3.putObject(new PutObjectRequest(bucketName, fileName, centeraClip.getInputStream(),meta));
			return centeraClip;
		}
		
		}catch (AdapterException e) {
				
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			
			log.severe("S3Adapter:error in HCP S3 Connection for file:"+fileName+"  "+stackTraceAsString);
			throw new AdapterException(e);
		} catch (Exception e) {
				
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			
			log.severe("S3Adapter:error in uploading file: "+fileName+"   "+stackTraceAsString);
			throw new AdapterException(e);
		} finally {
			try {
			
			log.info("S3Adapter:upload close input stream");
			if (compressedInput != null) compressedInput.close();
			if (byteArrayInputStream != null) byteArrayInputStream.close();
			
			}catch (IOException e) {
				log.severe("S3Adapter: closing in & out "+ e.getMessage());
				
		 	}
		}
		
	}
	
	public File getAsFile(String filename,char isCompressed) throws AdapterException {
	//Filename = GUID or GUID_index
		File file = null;
		File zipTempFile=null;
		
		try {
			AmazonS3 s3=getS3connection();
			
			if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");
				
			if (exists(filename)) {
				log.info("S3Adapter: Starting recalling file: "+filename);
				GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, filename);
				String temploc=System.getenv("TEMP_LOC_DIR")+"/";
				file = File.createTempFile("hcp", null);
				String storageFileName=file.getPath();
				String ziptmpFileName=null;
				if(isCompressed=='Y'){	
					 ziptmpFileName= temploc + filename+".zip";
					 zipTempFile = new File(ziptmpFileName);
					 s3.getObject(new GetObjectRequest(bucketName, filename), zipTempFile);
					 s3FileDecomression(ziptmpFileName,storageFileName,zipTempFile);
				}else{
					s3.getObject(new GetObjectRequest(bucketName, filename), file);
				}	
			}
			
			return file;
		}catch (AdapterException e) {
						
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			
			log.severe("S3Adapter:Error in Retrieving file:"+filename+"  "+stackTraceAsString);
			throw new AdapterException(e);
		} catch (Exception e) {
					
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			
			log.severe("S3Adapter:Error in Retrieving file from HCP S3:"+filename+"  "+stackTraceAsString);
			
			throw new AdapterException(e);
		}finally {
			
		}
		
		
	} 

	
	public InputStream getAsStream(String filename,char isCompressed) throws AdapterException {
		S3ObjectInputStream objectContent=null;
		
		try {
			AmazonS3 s3=getS3connection();
			
			if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");
		
			
		log.info("S3Adapter:getAsStream Starting recalling file: "+filename);	
		if(isCompressed=='N'){
					GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, filename);
					S3Object object = s3.getObject(getObjectRequest);
					objectContent = object.getObjectContent();
		}
		}catch (AdapterException e) {
			
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString(); 
			log.severe("S3Adapter:Error in Retrieving Stream from HCP S3: "+filename+"  "+stackTraceAsString);
			
			throw new AdapterException(e);
		} catch (Exception e) {
		
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			log.severe("S3Adapter:Error in downloading Stream from HCP S3: "+filename+"  "+stackTraceAsString);
			
			throw new AdapterException(e);
		}finally {
			
		}
		return objectContent;
		
	} 
	
	
	public boolean exists(String filename) throws AdapterException {
		AmazonS3 s3=getS3connection();
		
		if (filename == null || filename.length() == 0) return false;
		
		try {
			
			if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");
			
			Boolean exists = s3.doesObjectExist(bucketName, filename);
			if(exists){
				log.info("S3Adapter:File Exist Filename-"+filename);
				return true;
			}
			log.info("S3Adapter:File doesnot Exist Filename-"+filename);
		} catch (Exception e) {
					
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			
			log.severe("S3Adapter:Error in checking file Exist or not from HCP S3: "+stackTraceAsString);
			
			throw new AdapterException(e);
		} finally {
			
		}
		return false;
	}
	
	public Integer delete(String filename) throws AdapterException {
		AmazonS3 s3=getS3connection();
		
		if (filename == null || filename.length() == 0) return -1;
		
		try {
			
			if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");
			
			Boolean exists = s3.doesObjectExist(bucketName, filename);
			if(exists){
				log.info("S3Adapter:File Exist to delete Filename- "+filename);
				s3.deleteObject(bucketName, filename);
				return 1;
			}
			log.info("S3Adapter:File doesnot Exist to delete Filename- "+filename);
		} catch (Exception e) {
			
			StringWriter stringWriter= new StringWriter();
			PrintWriter printWriter= new PrintWriter(stringWriter);
			e.printStackTrace(printWriter);
			String stackTraceAsString= stringWriter.toString();
			
			log.severe("S3Adapter:Error in deleting file from HCP S3: "+filename+"   "+stackTraceAsString);
			
		} finally {
			
		}
		return -1;
	}
	
	private static byte[] getByteByInputStream(InputStream is)throws IOException{
		int len;
	    int size = 1024;
	    byte[] buf;	
		
	      ByteArrayOutputStream bos = new ByteArrayOutputStream();
	      buf = new byte[size];
	      while ((len = is.read(buf, 0, size)) != -1)
	         bos.write(buf, 0, len);
	      
	      buf = bos.toByteArray();
	      return buf;
	}
	private static void s3FileDecomression(String ziptmpFileName, String tmpFileName,File zipTempFile ){
	
		 try{ 
			 
			 FileInputStream fin = new FileInputStream(ziptmpFileName);
			 OutputStream output = new FileOutputStream(tmpFileName);
			 CompressionUtility.decompress(output,fin);
			 
			 try{
				 if(output != null) {
				    output.close();
				    }
				 if(fin != null) {
					 fin.close();
					}
				 if (zipTempFile != null) {
					 zipTempFile.delete();
								
						final Path tmpPath = zipTempFile.toPath();
						
						new Object() {
							@Override protected void finalize() throws Throwable {
								try {
									Files.deleteIfExists(tmpPath);
									log.info("s3FileDecomression File Deleted successfully -"+tmpPath);
								} catch (IOException e) {
									log.severe("s3FileDecomression error while deleting zip file "+ e);
									log.severe(e.getMessage());
									if (tmpPath!= null) tmpPath.toFile().deleteOnExit();
								}
							}
						};
						
			    		}else{
			    			log.info("s3FileDecomression Delete operation is failed.");
			    		}
			 }catch(IOException ie){
				 log.info("S3Adapter read: error in deleting file "+ ie );
							 
				}
			}
			catch(IOException ie){
				log.info("s3FileDecomression error while closing in or output"+ie);
				
			}
	}
	
	
	public void read(String filename, char isCompressed, OutputStream out){
		InputStream in =null;
		File zipTempFile=null;
		FileInputStream fin=null;
		 try{ 
			 AmazonS3 s3=getS3connection();
				if(s3==null)throw new AdapterException("S3Adapter: HCP s3 object is null");

				if (exists(filename)) {
					String temploc=System.getenv("TEMP_LOC_DIR")+"/";
					String ziptmpFileName=null;
					if(isCompressed=='Y'){	
						log.info("S3Adapter read: Compressed File: "+filename);
						ziptmpFileName= temploc + filename+".zip";
						zipTempFile = new File(ziptmpFileName);
						s3.getObject(new GetObjectRequest(bucketName, filename), zipTempFile);
						fin = new FileInputStream(zipTempFile);
						CompressionUtility.decompress(out,fin);
						 try{
							 if (fin != null) fin.close();
								zipTempFile.delete();
							}catch(IOException ie){
								log.info("S3Adapter read: problem in deleting file "+ie.getMessage());
							}
						
					}else{
						log.info("S3Adapter read: Non Compresses File:"+filename);
						GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, filename);
						S3Object object = s3.getObject(getObjectRequest);
						in = object.getObjectContent();
						int read = -1;
							 byte[] buffer = new byte[16384];
								 while ((read = in.read(buffer)) >0) {
								    out.write(buffer, 0, read);
							    }
						out.flush(); 
					}	
				}
			}
			catch(Exception e){
							
				StringWriter stringWriter= new StringWriter();
				PrintWriter printWriter= new PrintWriter(stringWriter);
				e.printStackTrace(printWriter);
				String stackTraceAsString= stringWriter.toString();
				log.severe("S3Adapter read: problem while reading file from HCP S3 "+filename+"  "+stackTraceAsString);
			}finally {
				log.info("S3Adapter read: In Finally closing out and IN object" );
				try{
					if (out != null) out.close();
				    if (in != null) in.close();
				}catch(IOException ie){
					log.info("S3Adapter error while closing In  or out stream"+ie);
				}
			}
	}

}
