package controllers;

import java.io.BufferedOutputStream;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import models.storageapp.AIMSRegistration;
import models.storageapp.AccessData;
import models.storageapp.AppConfigProperty;
import models.storageapp.Helper;
import models.storageapp.Metadata;
import models.storageapp.MetadataHistory;
import models.storageapp.Storage;
import play.Logger;
import play.Play;
import play.cache.Cache;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import utilities.Constant;
import utilities.DateUtil;
import utilities.Guid;
import utilities.MimeUtil;
import utilities.StringUtil;
import utilities.Utility;
import valueobjects.ArchiveFileRegistrationVO;
import ws.AIMSWSClient;
import adapters.AdapterException;
import adapters.aims.AimsAdapter;
import adapters.aims.AimsProject;
import adapters.aims.Ait;
import adapters.cas.CenteraAdapter;
import adapters.cas.CenteraClip;
import adapters.json.JsonAdapter;
import adapters.aws.S3Adapter;
import adapters.aws.HCPAdapter;
import com.avaje.ebean.Ebean;
import com.avaje.ebean.Page;
import com.avaje.ebean.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;

//s3
import com.amazonaws.services.s3.AmazonS3;
public class MetadataController extends Controller{

	//private static final Logger.ALogger Logger = Logger.of(MetadataController.class);


	private static String CAS_POOL_ADDRESS = Play.application().configuration().getString("cas.pool.address");
	private static String CAS_DATE_FORMAT = Play.application().configuration().getString("cas.date.format");
	private static String FILE_ROOT_DIR = Play.application().configuration().getString("file.root.dir");

	private static Form<Metadata> metadataForm = Form.form(Metadata.class);

	//private static CenteraAdapter adapter = new CenteraAdapter();
	private static String appName = Play.application().configuration().getString("app.name");
	private static String appVersion = Play.application().configuration().getString("app.version");

/*	static {
		adapter.setPoolAddress(CAS_POOL_ADDRESS);
		adapter.setDateFormat(CAS_DATE_FORMAT);
		adapter.
		adapter.
	}*/

	private static long streamMinSize = 2147483647L;
	static {
		try {
			streamMinSize = Long.parseLong(System.getProperty("stream.min.size"));
		} catch (Throwable e) {
			Logger.error("MetadataController - Unable to get stream.min.size property", e);
			e.printStackTrace();
		}
	}

	private static String streamBaseUrl = System.getProperty("stream.base.url");

	private static AimsAdapter aimsAdapter = AimsAdapter.getInstance();

	private static Result list(long offset, long length, String projectid) {
		return ok(Json.toJson(Metadata.list(offset, length, projectid)));
	}

	public static Result getArchiveData(String guid, String projectid) {
		Metadata data = Metadata.findByGuid(guid, projectid);

		if (data != null) {
			return ok(Json.toJson(data));
		} else {
			return notFound("File Not Found");
		}
	}   

	public static int deleteExtendedMetadata(String extendedMetadataUri,String indexPoolString, int status, String privDelMessage){
	 int indexStatus=1;
		try{
		if(status == 1 && 
				extendedMetadataUri != null && 
				extendedMetadataUri.length() > 0) {
			
			CenteraAdapter indexAdapter = new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,	appName, appVersion);
			
			//delete index file
			if(privDelMessage!=null){
				 indexStatus = indexAdapter.auditedDelete(extendedMetadataUri,privDelMessage);
				 Logger.info("MetaController:Delete Priveledge delete index file status "+indexStatus);
			}else{
				indexStatus = indexAdapter.delete(extendedMetadataUri);
				Logger.info("MetaController:Delete Normal delete index file index status "+indexStatus);
			}
			
		}
		}catch (Exception e) {
			e.printStackTrace();
			Logger.error(e.getMessage());
			return -1;
		}
		return 1;
	}
	
	public static Result deleteArchive(String guid, String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		Map<String, String> result = new HashMap<String, String>();
		String extendedMetadataUri=null;
		String indexPoolString=null;
		try {
			String storageUri = null;            
			Metadata data = Metadata.findByGuid(guid, projectid);

			if(data != null){
				if("DELETED".equals(data.getStatus()))
				{
					Logger.info(remoteAddress + "Archive already deleted.");
					return badRequest("Archive already deleted.");
				}
				else
				{
					
					//TD-DO
					Storage storageD=Helper.getArchiveStorageDetails(data);
					Logger.debug("MetadataController:deleteArchive storageD: "+storageD);
					Integer StorageTypeId=Helper.getStorageType(storageD);
					Logger.debug("MetadataController:deleteArchive storageType: "+StorageTypeId);
					//check the storage type, if 1 delete from centera
				
					//delete archive
					int status = 1;
					
					if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				    
					storageUri = Helper.getStorageUri(data);	
				    status=deleteClipsfromCentera(status,guid,storageUri,extendedMetadataUri,indexPoolString,data);

					
				}//end of centera
				else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
					
					     status=deleteObjectFromHCP(status,guid,storageD,extendedMetadataUri,indexPoolString,data);
					
					}
					
					if(status >= 1){
					data.setStatus("DELETED");
					data.setModifiedBy(request().username());
					data.setModificationTimestamp(Utility.getCurrentTime());
					Metadata.update(data);
					result = Utility.getMessage("true", "delete", guid + " has been deleted succesfully.");

					Logger.info(remoteAddress + "deleted archive file (" + guid + ") with storageUri (" + storageUri + ") by user " + request().username());
					}
					else{
						result = Utility.getMessage("false", "delete", guid + " has not been deleted");
					}
				}
			}
			else
			{
				Logger.info(remoteAddress + "Archive with GUID Not Found.");
				return notFound("Archive with GUID Not Found.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}

		return ok(Json.toJson(result));
	}


	public static Result privilegedDeleteArchive(String guid, String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		Map<String, String> result = new HashMap<String, String>();
		try {
			String storageUri = null;            
			Metadata data = Metadata.findByGuid(guid, projectid);

			if(data != null){
				if("DELETED".equals(data.getStatus()))
				{
					Logger.info(remoteAddress + "Archive already deleted.");
					return badRequest("Archive already deleted.");
				}
				else
				{
					storageUri = Helper.getStorageUri(data);
					if(storageUri != null && storageUri.length() > 0) {
						String archivePoolString = Helper.getArchiveStorageCASPoolString(data);
						CenteraAdapter archiveAdapter = 
								new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
										appName, appVersion);
						//int status = archiveAdapter.delete(storageUri);
						int status = archiveAdapter.auditedDelete(storageUri,"PRIVILEGED DELETE");
						String extendedMetadataUri = Helper.getExtendedMetadata(data);
						if(status == 1 && 
								extendedMetadataUri != null && 
								extendedMetadataUri.length() > 0) {
							String indexPoolString = 
									Helper.getIndexFileStorageCASPoolString(data);
							CenteraAdapter indexAdapter = 
									new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
											appName, appVersion);
							indexAdapter.auditedDelete(extendedMetadataUri,"PRIVILEGED DELETE");
						}
					}

					data.setStatus("DELETED");
					data.setModifiedBy(request().username());
					data.setModificationTimestamp(Utility.getCurrentTime());
					Metadata.update(data);
					result = Utility.getMessage("true", "privilegedDelete", guid + " has been deleted succesfully.");

					Logger.info(remoteAddress + "privileged deleted archive file (" + guid + ") with storageUri (" + storageUri + ") by user " + request().username());
					System.out.println(remoteAddress + "privileged deleted archive file (" + guid + ") with storageUri (" + storageUri + ") by user " + request().username());
				}
			}
			else
			{
				Logger.info(remoteAddress + "Archive with GUID Not Found.");
				return notFound("Archive with GUID Not Found.");
			}
		} catch (Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}

		return ok(Json.toJson(result));
	}
	
	
	public static Result getClipIdInfo(String projectid, String clipid) {
        String output = "NOT FOUND";
        try{
        	String archivePoolString = (String)Cache.get(Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY);
       		CenteraAdapter archiveAdapter = new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT, appName, appVersion);
       		CenteraClip clip = archiveAdapter.get(clipid);
            if(clip != null){
            	output=clip.toStringExtended();
                System.out.println("******************clipInfo"+output);     
            }else{
            	System.out.println("******************clip does not exists for clipid "+clipid);    
            }
               
        }catch (Exception e){
               System.out.println("Error in getting clipID Info" + e);
        }
        
        
       
		
        
        return ok(output);
 } 

	
	public static Result archive(String projectid) {
		Logger.info("Entered MetadataController archive");
		String remoteAddress = request().remoteAddress() + " - ";
		long maturityDate = 0L;
		String maturitydateParam = request().getQueryString("maturitydate");
		if (maturitydateParam == null || "".equals(maturitydateParam)) {
			Logger.error(remoteAddress + "Missing manatory maturitydate parameter");
			return badRequest("Missing or invalid manatory maturitydate parameter");
		}
		try {
			maturityDate = DateUtil.parseStringToTime(Constant.MATURITY_DATE_FORMAT, maturitydateParam);
		} catch (Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return badRequest("Missing or invalid mandatory maturitydate parameter");
		}
		
		
		long contentLength = 0L;
		try {
			contentLength = Long.parseLong(request().getHeader("Content-Length"));
		} catch (Throwable e) {}
		if (contentLength > Constant.PLAY_MAX_SIZE) {
			Logger.error(remoteAddress + "Upload file is too big");
			return badRequest("Upload file is too big");
		}

		AimsProject aimsProject = null;
		try {
			aimsProject = aimsAdapter.getProjectDetails(projectid);
		} catch(Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}
		if (aimsProject == null) {
			Logger.error(remoteAddress + "not project with ID " + projectid);
			return badRequest();
		}

		String namespace = request().getQueryString("namespace");
		if (namespace == null) namespace = "";

		String filename = MimeUtil.getFilenameFromContentDispositionHeader(request().getHeader("Content-Disposition"));
		if (Utility.isNullOrEmpty(filename)) {
			Logger.error(remoteAddress + "Content-Disposition header is required");
			return badRequest("Content-Disposition header is required");
		}

		String contentType = request().getHeader("Content-Type");
		if (!"application/octet-stream".equalsIgnoreCase(contentType)) {
			Logger.error(remoteAddress + "Invalid Content-Type");
			return badRequest("Invalid Content-Type");
		}

		Path localPath = null;
		final String filepath = request().getHeader("X-File-Path");
		if (filepath != null && filepath.length() > 0) {
			localPath = Paths.get(filepath);
			if (Files.notExists(localPath)) {
				Logger.error(remoteAddress + "X-File-Path file does not exist");
				return badRequest("X-File-Path file does not exist");
			}
		} else {
			Http.RequestBody reqBody = request().body();
			if (reqBody == null) return badRequest("Bad Request");
			else if (reqBody.isMaxSizeExceeded()) return badRequest("Upload file is too big");

			Http.RawBuffer bodyRaw = reqBody.asRaw();
			if(bodyRaw != null && bodyRaw.size() > 0) {
				localPath = bodyRaw.asFile().toPath();
			} else {
				Logger.error(remoteAddress + "Zero byte raw file size");
				return badRequest("Zero byte raw file size");
			} 
		}

		//save to centera
		final long startTime = System.currentTimeMillis();
		final String guid = Guid.guid(projectid);
		
		String toCompress=request().getHeader("X-Compression-Enabled");
		if(toCompress==null){
			toCompress="false";
		}


		CenteraClip clip = new CenteraClip(maturityDate, aimsProject.recordCode.recordCodeSeconds(), aimsProject.recordCode.code, aimsProject.recordCode.countryCode);
		clip.filename = filename;
		clip.projectid = projectid;
		clip.creationUser = request().username();
		clip.guid = guid;
		Logger.info("MetadataController:Archive projectid:"+projectid+" filename:"+filename+" guid:"+guid+" FileSize:"+contentLength+"  Compress:"+toCompress);
		try {
			clip.setInputPath(localPath);
			clip.setCompressionEnabled(toCompress);

			Integer StorageTypeId = Integer.valueOf(
	        		(Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE));
			Logger.debug("MetadataController: Archive storageType: "+StorageTypeId);
			
			if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				String archivePoolString = (String)Cache.get(Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY);
				CenteraAdapter archiveAdapter = 
					new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
							appName, appVersion);
				Logger.info("MetadataController:Archive Calling CenteraAdapter to upload  "+guid);
				clip = archiveAdapter.put(clip);
			
			}else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
				
				//HCP AND S3 Configurations details
				clip.size = contentLength;
				String s3EndUrl=(String)Cache.get(Constant.HCP_CURRENT_S3_END_URL);
				String hcpEndUrl=(String)Cache.get(Constant.HCP_CURRENT_REST_END_URL);
				String bucketName=(String)Cache.get(Constant.HCP_CURRENT_BUCKET);
				String accessKey=(String)Cache.get(Constant.HCP_CURRENT_ACCESS_KEY);
				String secretKey=(String)Cache.get(Constant.HCP_CURRENT_SECRET_KEY);
				Logger.info("MetadataController:Archive Calling S3dapter to upload  "+guid);
				S3Adapter s3adapter = new S3Adapter(s3EndUrl,bucketName,accessKey,secretKey);
				clip=s3adapter.upload(clip,clip.guid,contentLength);
						
				//applying retention
				HCPAdapter hcpAdapter=new HCPAdapter(hcpEndUrl,accessKey,secretKey);
				hcpAdapter.applyRetentionEndDate(clip.guid,Utility.getRetentionEndInSeconds(clip.getRetentionPeriod(), clip.getRetentionEnd()));
			}else{
				throw new AdapterException("Metadacontroller: no Active Storge type found");
			}
			
			clip.ingestionStart = startTime;	 
			clip.ingestionEnd = System.currentTimeMillis();
		
			Logger.debug("MetadataController: local path - "+localPath);
		} catch (AdapterException e) {
			Logger.error(remoteAddress + e.getMessage());
			if(e.getMessage().contains("An error in the generic stream occurred") && (request().getHeader("X-Compression-Enabled")!=null && request().getHeader("X-Compression-Enabled").equalsIgnoreCase("true")) ){
				return badRequest("Error occured due to one of the following reasons"
						+ "\nTrying to compress an already compressed file");
			}
			return internalServerError();
		}catch (IOException e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		} finally {
			try {
				Logger.debug("MetadataController:archive Deleting file from temporary location! - "+localPath);
				boolean deleted=Files.deleteIfExists(localPath);
			} catch (IOException e) {
				Logger.info("MetadataController: "+remoteAddress + e.getMessage());
				if (localPath != null) localPath.toFile().deleteOnExit();
			}
		}

		//setting metadata value from centera clip
		if (aimsProject.ait != null && aimsProject.ait != Ait.DEFAULT) {
			clip.ait = aimsProject.ait.id;
		}

		clip.sourceServer = request().remoteAddress();
		clip.userAgent = request().getHeader("User-Agent");
		clip.sourceNamespace = (namespace != null ? namespace : "");
		clip.setCompressionEnabled(toCompress);
		
		clip.preCompressedSize = contentLength;

		String locationUrl = getUrl(guid, projectid, clip.size);

		Metadata metadata = 
				saveMetadataAndRegisterwithAIMS(Guid.guid(), locationUrl, clip,null);

		Logger.debug("new archive: " + metadata.toString());

		response().setHeader("Location", locationUrl);
		response().setHeader("x-guid", metadata.getGuid());

		if (request().accepts("application/json")) return created(Json.toJson(metadata));
		else {
			StringBuilder result =  new StringBuilder();
			result.append(metadata.getGuid()).append("\t").append(metadata.getLocationUrl());
			return created(result.toString());
		} 
	}        
	
	public static Result eventBasedArchive(String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		String eventBased=Constant.ENABLED;

		long contentLength = 0L;
		try {
			contentLength = Long.parseLong(request().getHeader("Content-Length"));
		} catch (Throwable e) {}
		if (contentLength > Constant.PLAY_MAX_SIZE) {
			Logger.error(remoteAddress + "Upload file is too big");
			return badRequest("Upload file is too big");
		}

		AimsProject aimsProject = null;
		try {
			aimsProject = aimsAdapter.getProjectDetails(projectid);
		} catch(Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}
		if (aimsProject == null) {
			Logger.error(remoteAddress + "not project with ID " + projectid);
			return badRequest();
		}
		if(!aimsProject.recordCode.usOfficialEventType.equalsIgnoreCase("Event + Fixed Time")){
			Logger.info("Record code is not event based");
			return badRequest("Record code is not event based");
		}
		
		String namespace = request().getQueryString("namespace");
		if (namespace == null) namespace = "";

		String filename = MimeUtil.getFilenameFromContentDispositionHeader(request().getHeader("Content-Disposition"));
		if (Utility.isNullOrEmpty(filename)) {
			Logger.error(remoteAddress + "Content-Disposition header is required");
			return badRequest("Content-Disposition header is required");
		}

		String contentType = request().getHeader("Content-Type");
		if (!"application/octet-stream".equalsIgnoreCase(contentType)) {
			Logger.error(remoteAddress + "Invalid Content-Type");
			return badRequest("Invalid Content-Type");
		}

		Path localPath = null;
		final String filepath = request().getHeader("X-File-Path");
		if (filepath != null && filepath.length() > 0) {
			localPath = Paths.get(filepath);
			if (Files.notExists(localPath)) {
				Logger.error(remoteAddress + "X-File-Path file does not exist");
				return badRequest("X-File-Path file does not exist");
			}
		} else {
			Http.RequestBody reqBody = request().body();
			if (reqBody == null) return badRequest("Bad Request");
			else if (reqBody.isMaxSizeExceeded()) return badRequest("Upload file is too big");

			Http.RawBuffer bodyRaw = reqBody.asRaw();
			if(bodyRaw != null && bodyRaw.size() > 0) {
				localPath = bodyRaw.asFile().toPath();
			} else {
				Logger.error(remoteAddress + "Zero byte raw file size");
				return badRequest("Zero byte raw file size");
			} 
		}

		//save to centera
		final long startTime = System.currentTimeMillis();
		final String guid = Guid.guid(projectid);
		
		String toCompress=request().getHeader("X-Compression-Enabled");
		if(toCompress==null){
			toCompress="false";
		}

		// Option 1, retention end date is calculated based on the record code
		//CenteraClip clip = new CenteraClip(startTime, aimsProject.recordCode.recordCodeSeconds(), aimsProject.recordCode.code, aimsProject.recordCode.countryCode);
		
		//Option 2 retention period is infinite and retention end date = -1, to indicate Permanent retention
		CenteraClip clip = new CenteraClip(startTime, -1, aimsProject.recordCode.code, aimsProject.recordCode.countryCode);
				
		clip.filename = filename;
		clip.projectid = projectid;
		clip.creationUser = request().username();
		clip.guid = guid;
		Logger.info("MetadataController:EventBasedArchive projectid:"+projectid+" filename:"+filename+" guid:"+guid+" FileSize:"+contentLength+"  Compress:"+toCompress);
		try {
			clip.setInputPath(localPath);
			clip.setCompressionEnabled(toCompress);
			clip.preCompressedSize = contentLength;
			
			Integer StorageTypeId = Integer.valueOf(
	        		(Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE));
			Logger.debug("MetadataController: Archive storageType: "+StorageTypeId);
			
			if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				String archivePoolString = (String)Cache.get(
						Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY);
				CenteraAdapter archiveAdapter = 
						new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
								appName, appVersion);
				Logger.info("MetadataController:eventBasedArchive Calling CenteraAdapter to upload  "+guid);
				clip = archiveAdapter.put(clip);
			}
			else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
				//HCP AND S3 Configurations details
				clip.size = contentLength;
				String s3EndUrl=(String)Cache.get(Constant.HCP_CURRENT_S3_END_URL);
				String hcpEndUrl=(String)Cache.get(Constant.HCP_CURRENT_REST_END_URL);
				String bucketName=(String)Cache.get(Constant.HCP_CURRENT_BUCKET);
				String accessKey=(String)Cache.get(Constant.HCP_CURRENT_ACCESS_KEY);
				String secretKey=(String)Cache.get(Constant.HCP_CURRENT_SECRET_KEY);
				Logger.info("MetadataController:eventBasedArchive Calling S3Adapter to upload  "+guid);
				S3Adapter s3adapter = new S3Adapter(s3EndUrl,bucketName,accessKey,secretKey);
				clip=s3adapter.upload(clip,clip.guid,contentLength);
				//applying retention
				HCPAdapter hcpAdapter=new HCPAdapter(hcpEndUrl,accessKey,secretKey);
				hcpAdapter.applyRetentionEndDate(clip.guid,Utility.getRetentionEndInSeconds(clip.getRetentionPeriod(), clip.getRetentionEnd()));
				
			}
			else{
				throw new AdapterException("Metadacontroller: no Active Storge type found");
			}
			
			clip.ingestionStart = startTime;
			clip.ingestionEnd = System.currentTimeMillis();
		} catch (AdapterException e) {
			Logger.error(remoteAddress + e.getMessage());
			if(e.getMessage().contains("An error in the generic stream occurred") && (request().getHeader("X-Compression-Enabled")!=null && request().getHeader("X-Compression-Enabled").equalsIgnoreCase("true")) ){
				return badRequest("Error occured due to one of the following reasons"
						+ "\nTrying to compress an already compressed file");
			}
			return internalServerError();
		}catch (IOException e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		} finally {
			try {
				Files.deleteIfExists(localPath);
			} catch (IOException e) {
				if (localPath != null) localPath.toFile().deleteOnExit();
			}
		}

		//setting metadata value from centera clip
		if (aimsProject.ait != null && aimsProject.ait != Ait.DEFAULT) {
			clip.ait = aimsProject.ait.id;
		}

		clip.sourceServer = request().remoteAddress();
		clip.userAgent = request().getHeader("User-Agent");
		clip.sourceNamespace = (namespace != null ? namespace : "");
		clip.setCompressionEnabled(toCompress);
		clip.preCompressedSize = contentLength;
		

		String locationUrl = getUrl(guid, projectid, clip.size);

		Metadata metadata = 
				saveMetadataAndRegisterwithAIMS(Guid.guid(), locationUrl, clip,eventBased);

		Logger.debug("new archive: " + metadata.toString());

		response().setHeader("Location", locationUrl);
		response().setHeader("x-guid", metadata.getGuid());

		if (request().accepts("application/json")) return created(Json.toJson(metadata));
		else {
			StringBuilder result =  new StringBuilder();
			result.append(metadata.getGuid()).append("\t").append(metadata.getLocationUrl());
			return created(result.toString());
		} 
	}

	public static Result eventTriggered(String guid, String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		
		long startDate = 0L;
		String startdateParam = request().getQueryString("maturitydate");
		if (startdateParam == null || "".equals(startdateParam)) {
			Logger.error(remoteAddress + "Missing manatory maturitydate parameter");
			return badRequest("Missing or invalid mandatory maturitydate parameter");
		}
		try {
			startDate = DateUtil.parseStringToTime(Constant.MATURITY_DATE_FORMAT, startdateParam);
		} catch (Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return badRequest("Missing or invalid mandatory maturitydate parameter");
		}
				
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid))
			return badRequest("Invalid parameters");
		
		AimsProject aimsProject = null;
		try {
			aimsProject = aimsAdapter.getProjectDetails(projectid);
		} catch(Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}
		if (aimsProject == null) {
			Logger.error(remoteAddress + "not project with ID " + projectid);
			return badRequest();
		}
		if(!aimsProject.recordCode.usOfficialEventType.equalsIgnoreCase("Event + Fixed Time")){
			Logger.info("Record code is not event based, trigger event cannot be sent");
			return badRequest("Record code is not event based, trigger event cannot be sent");
		}
		Metadata metadata = Metadata.findByGuid(guid, projectid);
		if(metadata == null)
			return notFound("Data with " + guid +" Not Found.");			
		
		if("USER_ERROR".equalsIgnoreCase(metadata.getStatus())){
			Logger.info(remoteAddress + "Archive is in USER_ERROR status, Trigger event cannot be sent");
			return badRequest("Archive is in USER_ERROR status, Trigger event cannot be sent");
		}
		if(Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased())){
			Logger.info(remoteAddress + "Archive is already triggered, Trigger event cannot be sent again");
			return badRequest("Archive is already triggered, Trigger event cannot be sent again");
		}
		
		if(null == metadata.getEventBased() || "".equals(metadata.getEventBased())){
			Logger.info("Archive is not event based enabled, trigger event cannot be sent");
			return badRequest("Archive is not event based enabled, trigger event cannot be sent");
		}
		metadata.setRetentionStart(startDate);
			
		Long recordCodePeriod = aimsProject.recordCode.recordCodeSeconds();
		metadata.setRetentionEnd(metadata.getRetentionStart() + recordCodePeriod * 1000L);
		metadata.setEventBased(Constant.TRIGGERED);
			
		// Option 2 Only ILM_METADATA table is updated with actual retention start dt and retention end date
	    metadata.setModifiedBy(request().username());
        metadata.setModificationTimestamp(Utility.getCurrentTime());
        Metadata.update(metadata);
             
		//update status in AIMS REGISTRATION TABLE to EVENT_BASED_UPDATE_PENDING
		updateAIMSRegistrationstatus(Helper.getMetadataId(metadata));
		
		Logger.debug("trigger event metadata: " + metadata.toString());

		if (request().accepts("application/json")) return created(Json.toJson(metadata));
		else {
			StringBuilder result =  new StringBuilder();
			result.append(metadata.getGuid()).append("\t").append(metadata.getLocationUrl());
			return created(result.toString());
		} 

		
	}

	private static void updateAIMSRegistrationstatus(String metadataId) {
		AIMSRegistration aimsInfo = null;
		//Check in AIMS_REGISTRATION Table if the metadata id exists
		aimsInfo = AIMSRegistration.findById(metadataId);
		if (aimsInfo == null || (aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS 
				&& aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE))
		{
			Logger.warn("updateAIMSRegistrationstatus() - Not able to retrieve AIMS guid in AIMS REGISTRATION Table for :"+metadataId +" with success status");
			return;
		}
			
		if(aimsInfo.getAimsGuid() == null || aimsInfo.getAimsGuid().isEmpty()) {
			Logger.warn("updateAIMSRegistrationstatus() - " +
					"Bad AIMS REGISTRATION Record for :" + 
					metadataId + " unable to update AIMS Regsitration info.");
			return;
		}
		aimsInfo.setStatus(Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_PENDING);
		aimsInfo.save();
				
	}
	

	public static Result eventBasedArchiveAsJSon(String guid, String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		JsonNode json = request().body().asJson();
		if(json == null) {
			Logger.error(remoteAddress + "Json request body is blank");
			return badRequest("400 Bad Request");
		} else {
			Logger.info("archiveAsJson: " + json.toString());
			CenteraClip clip = null;
			try {
				clip = Json.fromJson(json, CenteraClip.class);
			} catch (Throwable e) {
				Logger.error(e.getMessage());
				return badRequest("400 Bad Request");
			} 

			String locationUrl = getStreamUrl(clip.guid, projectid);

			Metadata metadata = 
					saveMetadataAndRegisterwithAIMS(Guid.guid(), locationUrl, clip,Constant.ENABLED);

			return ok(Json.toJson(metadata));
		}
	}

	public static Result updateMetadataAfterEventTriggeredAsJSon(String guid, String projectid, String startDate) {
		
		Metadata metadata = Metadata.findByGuid(guid, projectid);
		if(metadata == null)
			return badRequest("Metadata with " + guid +" Not Found.");	
		if("USER_ERROR".equalsIgnoreCase(metadata.getStatus())){
			Logger.info("Archive is in USER_ERROR status, Trigger event cannot be sent");
			return badRequest("400 Bad Request");
		}
		if(Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased())){
			Logger.info("Archive is already triggered, Trigger event cannot be sent again");
			return badRequest("400 Bad Request");
		}
		if(null == metadata.getEventBased() || "".equals(metadata.getEventBased())){
			Logger.info("Archive is not event based enabled, trigger event cannot be sent");
			return badRequest("400 Bad Request");
		}
		
		String remoteAddress = request().remoteAddress() + " - ";
		
		AimsProject aimsProject = null;
		try {
			aimsProject = aimsAdapter.getProjectDetails(projectid);
		} catch(Exception e) {
			Logger.error(remoteAddress + e.getMessage());
			return internalServerError();
		}
		
		if(!"".equals(startDate) && null != startDate){
			metadata.setRetentionStart(Long.parseLong(startDate));
		}
		
		metadata.setRetentionEnd(metadata.getRetentionStart() + aimsProject.recordCode.recordCodeSeconds() * 1000L);
		metadata.setEventBased(Constant.TRIGGERED);
		metadata.setModifiedBy(request().username());
        metadata.setModificationTimestamp(Utility.getCurrentTime());
	
        Metadata.update(metadata);
        Logger.info("metadata update done");
        updateAIMSRegistrationstatus(Helper.getMetadataId(metadata));
        return ok(Json.toJson(metadata));

	}
	
	public static Result archiveAsJSon(String guid, String projectid) {
		String remoteAddress = request().remoteAddress() + " - ";
		JsonNode json = request().body().asJson();
		if(json == null) {
			Logger.error(remoteAddress + "Json request body is blank");
			return badRequest("400 Bad Request");
		} else {
			Logger.info("archiveAsJson: " + json.toString());
			CenteraClip clip = null;
			try {
				clip = Json.fromJson(json, CenteraClip.class);
			} catch (Throwable e) {
				Logger.error(e.getMessage());
				return badRequest("400 Bad Request");
			} 

			String locationUrl = getStreamUrl(clip.guid, projectid);

			Metadata metadata = 
					saveMetadataAndRegisterwithAIMS(Guid.guid(), locationUrl, clip,null);

			return ok(Json.toJson(metadata));
		}
	}

	private static Metadata saveMetadataAndRegisterwithAIMS(
			String id, String locationUrl, CenteraClip clip, String eventBased) {

		// Save Metadata
		Logger.debug("Saving Metadata for GUID: " + Guid.guid() + 
				" Location URL: " + locationUrl +
				" Stirage Clip: " + clip.toStringExtended());
		Metadata metadata = Metadata.newInstance(Guid.guid(), locationUrl, clip,eventBased);
		metadata.save();

		/*// Register with AIMS
		try {
			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(
					Constant.AIMS_SINGLE_REGISTRATION_URI_KEY);
			
			String sourceSystemId = AppConfigProperty.getAppConfigValue(
					Constant.UDAS_SOURCE_SYSTEM_ID_KEY);

			if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
				Logger.info("AIMS Single Regsitration service URL not found in App Config." +
						 "Skipping AIMS registration");
				return metadata;
			}

			String aimsSingleRegistrationUrl = root + extention;
			aimsSingleRegistrationUrl = aimsSingleRegistrationUrl.replace(
					Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId);
			aimsSingleRegistrationUrl = 
					aimsSingleRegistrationUrl.replace(
							Constant.AIMS_URL_PROJECT_ID_STRING, clip.projectid);
			Logger.debug("Inline Reg URL: " + aimsSingleRegistrationUrl);

			ArchiveFileRegistrationVO archiveFileRegistrationVO = 
					new ArchiveFileRegistrationVO();
			archiveFileRegistrationVO.setLobArchiveFileId(clip.guid);
			archiveFileRegistrationVO.setRetentionStartDate(DateUtil.formatDateYYYY_MM_DD(
					new Date(clip.getRetentionStart())));
			archiveFileRegistrationVO.setArchiveSizeInBytes(clip.size);
			archiveFileRegistrationVO.setArchiveDescription(clip.filename);
			archiveFileRegistrationVO.setArchiveDate(DateUtil.formatDateYYYY_MM_DD(
					new Date(clip.ingestionEnd)));
			archiveFileRegistrationVO.setEventBased(eventBased);
			
			Logger.debug("ArchiveFileRegistrationVO: " + archiveFileRegistrationVO);

			ObjectMapper objectMapper = new ObjectMapper(); 
			JsonNode jsonNode = objectMapper.convertValue(
					archiveFileRegistrationVO, JsonNode.class);

			Logger.debug("JsonNode: " + jsonNode);

			JsonNode responseJsonNode = 
					AIMSWSClient.jerseyPostToAIMSAndGetJsonResponse(
							aimsSingleRegistrationUrl, jsonNode);

			ArchiveFileRegistrationVO responseArchiveFileRegistrationVO = null;
			if(responseJsonNode != null) {
				responseArchiveFileRegistrationVO =
						objectMapper.convertValue(responseJsonNode, 
								ArchiveFileRegistrationVO.class);
				Logger.debug("AIMS Response ArchiveFileRegistrationVO: " + 
						responseArchiveFileRegistrationVO);
				if(responseArchiveFileRegistrationVO != null &&
						responseArchiveFileRegistrationVO.getAimsGuid() != null &&
						!responseArchiveFileRegistrationVO.getAimsGuid().trim().isEmpty() &&
						responseArchiveFileRegistrationVO.getLobArchiveFileId() != null &&
						!responseArchiveFileRegistrationVO.getLobArchiveFileId().trim().isEmpty()) {
					
					AIMSRegistration aimsRegistration = AIMSRegistration.newInstance(
							responseArchiveFileRegistrationVO, metadata);
					aimsRegistration.save();
					Logger.debug("AIMSRegistration saved: " + 
							aimsRegistration);
					metadata.setAimsRegistrationStatus(aimsRegistration.getStatus());
				} else {
					Logger.warn("AIMSRegistration NOT saved: receievd bad response from AIMS " + 
							responseJsonNode);
				}
			}

		} catch (Exception e) {
			Logger.error("MetadataController - Exception occurred while " +
					"registering archive to AIMS", e);
			e.printStackTrace();
		}*/

		return metadata;
	}

	private static String getUrl(String guid, String projectid, long size) {
		String locationUrl = "";
		if (size < streamMinSize || streamBaseUrl == null || streamBaseUrl.length() == 0) {
			locationUrl = controllers.routes.MetadataController.downloadFile(guid, projectid).absoluteURL(request());
			String httpsPort = System.getProperty("https.port");
			if (httpsPort != null && httpsPort.length() != 0) {
				locationUrl = locationUrl.replaceFirst("http:", "https:");
			}
		} else {
			locationUrl = streamBaseUrl + "/stream/1/archives?projectid=" + projectid + "&guid=" + guid;
		} 
		return locationUrl;
	}

	private static String getStreamUrl(String guid, String projectid) {
		String locationUrl = null;

		if(streamBaseUrl != null && streamBaseUrl.length() != 0) {
			locationUrl = streamBaseUrl + "/stream/1/archives?projectid=" + projectid + "&guid=" + guid;
		}

		return locationUrl;
	}

	public static Result downloadFile(final String guid, String projectid)
	{
		Map<String, String> result = new HashMap<String, String>();
		if(Utility.isNullOrEmpty(guid))
			return badRequest("GUID is required.");

		String storageUri = null;            
		Metadata data = Metadata.findByGuid(guid, projectid);
		if(data != null){
			if("DELETED".equals(data.getStatus()))
			{
				return badRequest("GUID already deleted.");
			}
			else if (data.getFileSize() >= streamMinSize) {
				return badRequest("file size is bigger than the max limit supported by this application.");
			}
			else
			{
				Logger.info("MetadataController:downloadFile guid:"+guid+" projectid:"+projectid);
				Storage storageD=Helper.getArchiveStorageDetails(data);
				Logger.debug("MetadataController: storageD: "+storageD);
				Integer StorageTypeId=Helper.getStorageType(storageD);
				Logger.debug("MetadataController: storageType: "+StorageTypeId);
				//check the storage type, if 1 download from centera
				if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
					storageUri = Helper.getStorageUri(data);
					if(storageUri != null && storageUri.length() > 0) {
						//String archivePoolString = Helper.getArchiveStorageCASPoolString(data);
						String archivePoolString = Helper.getCASStorageConnectionStringbyStorage(storageD);
						CenteraAdapter archiveAdapter = 
								new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
										appName, appVersion);
						Logger.info("MetadataController:downloadFile Recalling file from centera guid:"+guid);
						return getFileFromCentera(archiveAdapter, storageUri,data.getFileName());
					}
					else 
						return badRequest("GUID Not Found.");
				}else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
					Logger.info("MetadataController:downloadFile Recalling file from HCP guid:"+guid);
					S3Adapter s3adapter = new S3Adapter(storageD.getHcpS3Url(),storageD.getBucketName(),storageD.getAccessKey(),storageD.getSecretKey());
					
					return getFileFromHCP(s3adapter,data.getGuid(),data.getFileName());
				
					}
				else
					return badRequest("GUID Not Found.");
				
			}
		}
		else
		{
			return badRequest("GUID Not Found.");
		}
	}  
	
	
	public static Result downloadFileUsingOldClip(final String guid, String projectid, String clipid)
	{
		Map<String, String> result = new HashMap<String, String>();
		if(Utility.isNullOrEmpty(guid))
			return badRequest("GUID is required.");

		//String storageUri = null;            
		Metadata data = Metadata.findByGuid(guid, projectid);
		if(data != null){
			//if("DELETED".equals(data.getStatus()))
			//{
			//	return badRequest("GUID already deleted.");
			//}
			//else if (data.getFileSize() >= streamMinSize) {
			//	return badRequest("file size is bigger than the max limit supported by this application.");
			//}
			//else
			//{
				//storageUri = Helper.getStorageUri(data);
				//if(storageUri != null && storageUri.length() > 0) {
					String archivePoolString = Helper.getArchiveStorageCASPoolString(data);
					CenteraAdapter archiveAdapter = 
							new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
									appName, appVersion);
					
					return getFileFromCentera(archiveAdapter, clipid, data.getFileName());
				/*}
				else 
					return badRequest("GUID Not Found.");
			}*/
		}
		else
		{
			return badRequest("GUID Not Found.");
		}
	}  
	

	private static Result getFileFromCentera(final CenteraAdapter centeraAdapter, 
			final String storageUri, String filename){

		File file = null;
		try {
			Metadata metadata = Metadata.findByStorageUri(storageUri);
			char isCompressed='N';
			if(metadata!=null){
				isCompressed=metadata.getIsCompressed();
			}
			file = centeraAdapter.getAsFile(storageUri,isCompressed);
			if (file != null) {               

				response().setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

				return ok(file);               
			} else {
				return notFound("404 Not Found");
			}
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return internalServerError(e.getMessage());
		} finally {
			if (file != null) {
				file.deleteOnExit();

				final Path tmpPath = file.toPath();

				new Object() {
					@Override protected void finalize() throws Throwable {
						try {
							Files.deleteIfExists(tmpPath);
						} catch (IOException e) {
							Logger.error(e.getMessage());
						}
					}
				};
			}
		}
	}  

	private static Result getFileFromHCP(S3Adapter s3Adapter,String hcpFilename, String fileName){
		
		File file = null;
		try {
			Metadata metadata = Metadata.findByStorageUri(hcpFilename);
			char isCompressed='N';
			if(metadata!=null){
				isCompressed=metadata.getIsCompressed();
			}
		
			file = s3Adapter.getAsFile(hcpFilename,isCompressed);
			if (file != null) {               

				response().setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");

				return ok(file);               
			} else {
				return notFound("404 Not Found");
			}
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return internalServerError(e.getMessage());
		} finally {
			if (file != null) {
				file.deleteOnExit();
				
				final Path tmpPath = file.toPath();

				new Object() {
					@Override protected void finalize() throws Throwable {
						try {
							Files.deleteIfExists(tmpPath);
						
						} catch (IOException e) {
							Logger.error(e.getMessage());
						}
					}
				};
			}
		}
	}  

	
	private static int addDateToParamList(List<String> criteriaList, List<Object> paramList, String queryPiece, String criteriaName)
	{

		if(queryPiece.contains("_bt_")) {
			String[] val = queryPiece.toLowerCase().split("_bt_");
			if(val.length == 2)
			{
				try
				{
					String from = DateUtil.parseStringToOracleDate(Constant.MATURITY_DATE_FORMAT, val[0]);
					String to = DateUtil.parseStringToOracleDate(Constant.MATURITY_DATE_FORMAT, val[1]);
					if(from == null || to == null) throw new Exception();
					// Date is stored as NUMBER containing Java/EPOCH timestamp, this converts it to a date for comparison
					// Use an upper and lower bound in case of invalid data
					// Most dates are NOT NULL, but better be paranoid and set NULL to 0
					criteriaList.add("TRUNC((CAST('01-JAN-1970' AS TIMESTAMP WITH LOCAL TIME ZONE)+LEAST(GREATEST(COALESCE(" + criteriaName + ", 0),-62135769600000),253402300799000)/86400000), 'DD') between TO_DATE(?, 'YYYY-MM-DD') and TO_DATE(?, 'YYYY-MM-DD')");
					paramList.add(from);
					paramList.add(to);
				}
				catch(Exception e)
				{	
					try{
						long from = Long.parseLong(val[0]);
						long to = Long.parseLong( val[1]);
						if(from < 0 && to < 0) throw new NumberFormatException();
						criteriaList.add(criteriaName + " between ? and ?");
						paramList.add(from);
						paramList.add(to);
					}
					catch(NumberFormatException ee)
					{
						return -1;
					}
				}
			}

		}
		else
		{
			String comp = "=";
			if(queryPiece.contains("gt_")) {
				comp = ">";
				queryPiece = queryPiece.replace("gt_", "");
			}
			else if(queryPiece.contains("le_")){
				comp = "<=";
				queryPiece = queryPiece.replace("le_", "");
			}
			else if(queryPiece.contains("lt_")){
				comp = "<";
				queryPiece = queryPiece.replace("lt_", "");
			}
			try
			{
				String val = DateUtil.parseStringToOracleDate(Constant.MATURITY_DATE_FORMAT, queryPiece);
				if(val == null) throw new Exception();
				// Date is stored as NUMBER containing Java/EPOCH timestamp, this converts it to a date for comparison
				// Use an upper and lower bound in case of invalid data
				// Most dates are NOT NULL, but better be paranoid and set NULL to 0
				criteriaList.add("TRUNC((CAST('01-JAN-1970' AS TIMESTAMP WITH LOCAL TIME ZONE)+LEAST(GREATEST(COALESCE(" + criteriaName + ", 0),-62135769600000),253402300799000)/86400000), 'DD') " + comp + " TO_DATE(?, 'YYYY-MM-DD')");
				paramList.add(val);
			}
			catch(Exception e)
			{	
				try
				{ 
					long val = Long.parseLong(queryPiece);
					if(val < 0) throw new NumberFormatException();
					criteriaList.add(criteriaName + " " + comp + " ?");
					paramList.add(val);
				}
				catch(NumberFormatException ee)
				{
					return -1;
				} 
			}
		}
		return 0;
	}

	private static String getStringValue(String criteriaString, String operator)
	{
		return criteriaString.toLowerCase().split(operator)[1];
	}

	private static long getLongValue(String criteriaString, String operator)
	{
		String[] value = criteriaString.split(operator);
		String longValue = "";

		if(value.length > 1)
			longValue = value[1];
		else
			return -1;

		long val = -1;

		try{
			val = DateUtil.parseStringToTime(Constant.MATURITY_DATE_FORMAT, longValue);
		}
		catch(Exception e)
		{
			try
			{
				val = Long.parseLong(longValue);
			}
			catch(NumberFormatException ee)
			{
				return -1;
			}
		}

		return val;
	}

	public static Result doSearch(long offset, long length, String projectid) {

		String sortby = "ingestion_start";
		String order = "desc";

		if (offset < 0 || length < 1) {
			Logger.warn("invalid page number (offset) or page size (length)");
			return badRequest("invalid page number (offset) or page size (length)");
		}

		if (length > 10000 || (request().accepts("application/json") && length > 1000)) {
			Logger.error("length exceeds supported limit");
			return badRequest("length exceeds supported limit");
		}

		Map<String, String[]> params = request().queryString();

		List<String> criteriaList = new ArrayList<String>();
		List<Object> paramsList = new ArrayList<Object>();

		if(!Utility.isNullOrEmpty(projectid))
		{
			criteriaList.add("project_id = ?");
			paramsList.add(projectid);
		}


		Query<Metadata> query = Ebean.createQuery(Metadata.class);
		Boolean defaultStatus = true; // User didn't specify a status
		Boolean retentionStartProvided = false;
		if(params!=null || !params.isEmpty())
		{
			for (String key: params.keySet()) {
				String value = params.get(key)[0];
				if(Utility.isNullOrEmpty(value))
					return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				if(key.contains("filename"))
				{
					criteriaList.add("file_name_case like ?");
					if(value != null && value.length() > 0)
						paramsList.add("%" +value.toUpperCase() + "%");
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("country"))
				{
					criteriaList.add("country = ?");
					if(value != null && value.length() > 0)
						paramsList.add(value.toUpperCase());
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("status"))
				{
					defaultStatus = false;
					if(value != null && value.length() > 0) {
						defaultStatus = false;
						String[] statuses = value.split(",");
						StringBuilder sqlParam = new StringBuilder("status IN (");
						String joiner = "";
						for(String status : statuses) {
							paramsList.add(status.toUpperCase().trim());
							sqlParam.append(joiner);
							sqlParam.append("?");
							joiner = ", ";
						}
						sqlParam.append(")");
						criteriaList.add(sqlParam.toString());
					} else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else  if(key.contains("ingestionuser"))
				{
					criteriaList.add("ingestion_user_case = ?");
					if(value != null && value.length() > 0)
						paramsList.add(value.toUpperCase());
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("modifiedby"))
				{
					criteriaList.add("modified_by = ?");
					if(value != null && value.length() > 0)
						paramsList.add(value.toUpperCase());
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("recordcode"))
				{
					criteriaList.add("record_code = ?");
					if(value != null && value.length() > 0)
						paramsList.add(value.toUpperCase());
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("sortby"))
				{
					if(value != null && value.length() > 0)
					{
						if("ingestiondate".equals(value))
							sortby = "ingestion_start";
						else if("retentionstart".equals(value))
							sortby = "retention_start";
						else if("filename".equals(value))
							sortby = "file_name";
						else if("filesize".equals(value))
							sortby = "file_size";
					}
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("order"))
				{
					if(value != null && value.length() > 0)
					{
						if("desc".equalsIgnoreCase(value))
							order = "desc";
						else if("asc".equals(value))
							order = "asc";
					}
					else
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("filesize"))
				{
					if(value.contains("gt_")){
						criteriaList.add("file_size > ?");
						long val = getLongValue(value, "gt_");
						if(val >= 0)
							paramsList.add(val);
						else
							return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));

					}
					else if(value.contains("le_")){
						criteriaList.add("file_size <= ?");
						long val = getLongValue(value, "le_");
						if(val >= 0)
							paramsList.add(val);
						else
							return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
					}
					else
					{
						criteriaList.add("file_size = ?");

						long val = 0;
						try {
							val = Long.parseLong(value);
						} catch (NumberFormatException e) {
							val = -1;
						}
						if(val >= 0)
							paramsList.add(val);
						else
							return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
					}
				}
				else if(key.contains("ingestiondate"))
				{
					int result = addDateToParamList(criteriaList, paramsList, value, "ingestion_start");
					if(result < 0)
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("retentionstart") || key.contains("maturitydate") )
				{
					retentionStartProvided = true;
					int result = addDateToParamList(criteriaList, paramsList, value, "retention_start");
					if(result < 0)
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}
				else if(key.contains("retentionend"))
				{
					int result = addDateToParamList(criteriaList, paramsList, value, "retention_end");
					if(result < 0)
						return ok(Json.toJson(Utility.getMessage("false", "Get", "Invalid query parameter")));
				}   
			}
			if(defaultStatus) {
				criteriaList.add("status IN (?, ?)");
				paramsList.add("PENDING");
				paramsList.add("ARCHIVED");
			}
			if(retentionStartProvided){
				criteriaList.add("(EVENT_BASED is null or EVENT_BASED = '"+Constant.TRIGGERED+"')");
			}
			if(!criteriaList.isEmpty() && !paramsList.isEmpty())
			{   
				query.where(Utility.join(criteriaList, " AND "));
				Logger.info("Using search criteria: " + criteriaList);
				int j = 1; // first param uses index 1 NOT 0!
				for (Object param : paramsList) {
					query.setParameter(j, param);
					j++;
				}
			}
		}

		if("asc".equalsIgnoreCase(order))
			query.orderBy().asc(sortby);
		else
			query.orderBy().desc(sortby);

		//System.out.println("Query in controller: " + query.getGeneratedSql());

		if (request().accepts("application/json")) {
			List<Metadata> resultList = query.findPagingList((int)length).setFetchAhead(false).getPage((int)offset).getList();
			if(resultList!=null || resultList.size() > 0) {
				return ok(Json.toJson(resultList));
			} else return ok(Json.toJson(Utility.getMessage("false", "Get", "No result found")));
		} else {
			boolean xContent = "true".equalsIgnoreCase(request().getHeader("x-metadata-content"));
			return metadataAsCsvFile(query, (int)offset, (int)length, xContent);       
		}
	}
	
	public static Result isFileCompressed(String clipid){
		Metadata metadata = Metadata.findByStorageUri(clipid);
		return ok(Json.toJson(metadata.getIsCompressed()));
	}

	public static Result getStorageType(String guid, String projectid){
		
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) 
			return badRequest("Invalid parameters");

		Metadata data = Metadata.findByGuid(guid, projectid);

		if(data != null) {
			Storage storageD=Helper.getArchiveStorageDetails(data);
			Integer StorageTypeId=Helper.getStorageType(storageD);
			return ok(String.valueOf(StorageTypeId));
		}
	
		return notFound();
	}
	
	public static Result getActiveStorageType(){
		Logger.debug("getActiveStorageType: "+Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE));
		return ok(String.valueOf(Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE)));

	}
	
	
	private static Result metadataAsCsvFile(Query query, int pageNumber, int pageSize, boolean xContent) {
	
		final int maxChunkSize = 1000;
		int chunkSize = (pageSize > maxChunkSize ? maxChunkSize : pageSize);
		int chunkPageNumber = 0;
		int rowsToSkip = 0;
		if (pageNumber > 0) {
			int totalPrevRows = pageNumber * pageSize;
			chunkPageNumber = totalPrevRows / chunkSize;
			rowsToSkip = totalPrevRows % chunkSize;
		}
		int rowCount = 0;

		File file = null;
		Path path = null;
		NewlineFilterOutputStream out = null;
		byte[] tab = "\t".getBytes();
		byte[] line = System.getProperty("line.separator").getBytes();
		byte[] data = new byte[0];

		try {
			path = Files.createTempFile("metadata", null);
			out = new NewlineFilterOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
			Page page = null;
			do {
				if (page == null) page = query.findPagingList(chunkSize).setFetchAhead(false).getPage(chunkPageNumber);
				else page = page.next();
				List<Metadata> metadataList = page.getList();
				for(Metadata metadata : metadataList) {
					if (rowsToSkip-- > 0) continue;

					data = getData(metadata.getGuid());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getIngestionEnd());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getFileName());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getRecordCode());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getCountry());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getRetentionEnd());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getRetentionStart());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getLocationUrl());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);

					data = getData(metadata.getMetadataUrl());
					out.write(data, 0, data.length);
					out.write(tab, 0, tab.length);
					if (xContent) {
						Storage storageD=null;
						Integer StorageTypeId=-1;
						String clipid = Helper.getExtendedMetadata(metadata);
						Logger.debug("MetadataController:extended metadata clipid: "+clipid);
						if(clipid!=null){
							 storageD=Helper.getIndexStorageDetails(metadata);
							 StorageTypeId=Helper.getStorageType(storageD);
						}
						
						if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID && clipid !=null){
						try {
							
							String archivePoolString = Helper.getCASStorageConnectionStringbyStorage(
									storageD);
							CenteraAdapter archiveAdapter = 
									new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
											appName, appVersion);
							if (archiveAdapter.exists(clipid)) {
								Logger.debug("Metadata csv :centera- Fetching index file content");
								out.flush();
								out.on();
//								char isCompressed='N';
//								In case of index file, metadata will be null. In this case set isCompessed to 'N'
//								Metadata cmdt = Metadata.findByStorageUri(clipid);
//								if(cmdt!=null){
//									isCompressed=cmdt.getIsCompressed();
//								}
//								For Index files compression is not enabled. Hence pass it as 'N' wile reading the Index file
								archiveAdapter.read(clipid,'N', out);
								out.off();
							}
						} catch (Exception e) {
							Logger.error("Error while reading centera index file : "+e.getMessage());
							out.off();
							}
						}
						else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID && clipid !=null){
							try {
								S3Adapter s3adapter = new S3Adapter(storageD.getHcpS3Url(),storageD.getBucketName(),storageD.getAccessKey(),storageD.getSecretKey());
								String hcpIndexFilename=metadata.getGuid()+Constant.HCP_INDEX_FILE_NAME_PREFIX;;
								if (s3adapter.exists(hcpIndexFilename)) {
									Logger.debug("Metadata csv :hcp- Fetching index file content"+hcpIndexFilename);
									out.flush();
									out.on();

									InputStream is=s3adapter.getAsStream(hcpIndexFilename,'N');
									IOUtils.copy(is, out);
									out.off();
								}
							} catch (Exception e) {
								Logger.error("Error while reading HCP index file : "+e.getMessage());
								out.off();
								}
						}
					}

					out.write(line, 0, line.length);
					out.flush();
					rowCount++;
				}

			} while (page.hasNext() && rowCount < pageSize);

			file = path.toFile();
			return ok(file);
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return internalServerError("500 Internal Server Error");
		} finally {
			try {
				if (out != null) out.close();
			} catch (IOException e) {}

			if (file != null) {
				file.deleteOnExit();

				final Path tmpPath = path;
				new Object() {
					@Override protected void finalize() throws Throwable {
						try {
							Files.deleteIfExists(tmpPath);
						} catch (IOException e) {
							Logger.error(e.getMessage());
						}
					}
				};
			}
		}
	}

	private static byte[] getData(String input) {
		if (input == null) return new byte[0];
		return input.getBytes();
	}

	private static byte[] getData(Long input) {
		if (input == null) return new byte[0];
		return getData(input.toString());
	}

	public static Result setExtendedMetadata(String guid, String projectid) {

		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid))
			return badRequest("Invalid parameters");

		Metadata metadata = Metadata.findByGuid(guid, projectid);
		if(metadata == null)
			return notFound("Data with " + guid +" Not Found.");
		else if (metadata.getMetadataUrl() != null && metadata.getMetadataUrl().length() !=0) {
			return badRequest("Extended Metadata already exists");
		}

		if(Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased())){
			return badRequest("Archive is already triggered, Extended metadata cannot be pushed");
		}
		long contentLength = 0L;
		try {
			contentLength = Long.parseLong(request().getHeader("Content-Length"));
		} catch (Throwable e) {}
		if(contentLength > Math.max(Constant.EXTENDED_METADATA_SIZE_LIMIT, Math.min(Constant.PLAY_MAX_SIZE, metadata.getFileSize() * 0.1))) {
			return badRequest("Extended Metadata size is larger than max limit.");
		}

		File metadataFile = null;
		if(request().body() != null) {
			Http.RawBuffer raw = request().body().asRaw();
			if (raw != null) {
				metadataFile = raw.asFile();
			}			
		}

		if(metadataFile == null) return badRequest("No request body");
		// START: 02/09/2017: Narayana for perm retention fix 
		/* For fixed retention, index file ret start date and end date are populated with data file ret end date
		   inccase of perm retention, since data file retention end date is NULL, above logic is failing
		   following fix is to populate retention start date as retention start date for data file and end date is -1 ie permanent
		*/
		
				String remoteAddress = request().remoteAddress() + " - ";
			
				CenteraClip clip = null;
				AimsProject aimsProject = null;
				try {
					aimsProject = aimsAdapter.getProjectDetails(projectid);
			
				} catch(Exception e) {
					Logger.error(remoteAddress + e.getMessage());
					return internalServerError();
				}
				
				if (aimsProject == null) {
					Logger.error(remoteAddress + "not project with ID " + projectid);
					return badRequest();
				}
				else{
					 if (aimsProject.recordCode.recordCodeSeconds() == -1 || Constant.ENABLED.equalsIgnoreCase(metadata.getEventBased()))  // meaning permanent retention project
						 clip = new CenteraClip(metadata.getRetentionStart(), -1, metadata.getRecordCode(), metadata.getCountry());
					 else
						 clip = new CenteraClip(metadata.getRetentionEnd(), 0L, metadata.getRecordCode(), metadata.getCountry());
				}
					
		// save to centera
		//CenteraClip clip = new CenteraClip(metadata.getRetentionEnd(), 0L, metadata.getRecordCode(), metadata.getCountry());
		//END: 02/09/2017: Narayana N for perm retention fix 

		
		clip.filename = StringUtil.encodeToBase64(guid);
		clip.projectid = projectid;
		clip.creationUser = request().username();
		clip.guid = guid;
		Logger.info("MetadataController setExtendedMetadata: projectid:"+projectid+" guid:"+guid+" FileSize:"+contentLength);
		try {
			clip.setInputPath(metadataFile.toPath());
		
			Integer StorageTypeId = Integer.valueOf(
	        		(Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE));
			Logger.debug("MetadataController: Archive storageType: "+StorageTypeId);
			
			if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				String indexPoolString = (String)Cache.get(
						Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY);
				CenteraAdapter indexAdapter = 
						new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
								appName, appVersion);
				Logger.info("MetadataController setExtendedMetadata: calling CenteraAdapater to upload index file " + guid);
				clip = indexAdapter.put(clip);
				metadata.setExtendedMetadata(clip.clipid);
			}
		else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
			Logger.debug("MetadataController contentLength2: " + contentLength);
			//HCP AND S3 Configurations details
		
			String s3EndUrl=(String)Cache.get(Constant.HCP_CURRENT_S3_END_URL);
			String hcpEndUrl=(String)Cache.get(Constant.HCP_CURRENT_REST_END_URL);
			String bucketName=(String)Cache.get(Constant.HCP_CURRENT_BUCKET);
			String accessKey=(String)Cache.get(Constant.HCP_CURRENT_ACCESS_KEY);
			String secretKey=(String)Cache.get(Constant.HCP_CURRENT_SECRET_KEY);
			
			//s3 adpater
			S3Adapter s3adapter = new S3Adapter(s3EndUrl,bucketName,accessKey,secretKey);
			String indexFileName=clip.guid+Constant.HCP_INDEX_FILE_NAME_PREFIX; // unique index filename eg. guid_index
			Logger.info("MetadataController setExtendedMetadata: calling s3adapater to upload index file " + guid);
			clip=s3adapter.upload(clip,indexFileName,contentLength);
		
			//applying retention
			
			HCPAdapter hcpAdapter=new HCPAdapter(hcpEndUrl,accessKey,secretKey);
			hcpAdapter.applyRetentionEndDate(indexFileName,Utility.getRetentionEndInSeconds(clip.getRetentionPeriod(), clip.getRetentionEnd()));
			metadata.setExtendedMetadata(indexFileName); //HCP unique index filename eg. guid_index
		}
		else{
			throw new AdapterException("Metadacontroller: no Active Storge type found");
		 }
			
		} catch (AdapterException e) {
			return internalServerError(e.getMessage());
		}catch (IOException e) {
			return internalServerError(e.getMessage());
		} finally {
			try {
				Files.deleteIfExists(metadataFile.toPath());
			} catch (IOException e) {
				if (metadataFile != null) metadataFile.deleteOnExit();
			}
		}

		String locationUrl = controllers.routes.MetadataController.getExtendedMetadata(guid, projectid).absoluteURL(request());
		String httpsPort = System.getProperty("https.port");
		if (httpsPort != null && httpsPort.length() != 0) {
			locationUrl = locationUrl.replaceFirst("http:", "https:");
		}

		metadata.setMetadataUrl(locationUrl);
		metadata.setMetadataSize(contentLength);
        
        // Set Storage
		//Active current storage id
		Integer CURRENT_WRITE_STORAGE_ID = Integer.valueOf((Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_KEY));
        
		
		metadata.setIndexFileStorageId(CURRENT_WRITE_STORAGE_ID);
		
		Metadata.update(metadata);

		//update ARCHIVE_FLIE_MASTER table's index_file_size entry
		updateAIMSRegistrationMasterInfo(Helper.getMetadataId(metadata),clip);

		response().setHeader("Location", locationUrl);

		if (request().accepts("text/csv")) return ok(locationUrl);
		else {
			String body = "{\"location\":\"" + locationUrl + "\"}";
			return ok(body);
		}	   	
	} 

	public static Result getExtendedMetadata(String guid, String projectid) {        
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) return badRequest("Invalid parameters");

		Metadata metadata = Metadata.findByGuid(guid, projectid);

		if(metadata == null)
			return notFound("Data with " + guid +" Not Found.");
		else{
			
			Storage storageD=Helper.getIndexStorageDetails(metadata);
			Logger.debug("MetadataController: storageD: "+storageD);
			Integer StorageTypeId=Helper.getStorageType(storageD);
			Logger.debug("MetadataController:downloadExtendedMetadata storageType: "+StorageTypeId);
			//check the storage type, if 1 download from centera
			if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				//String indexPoolString = Helper.getIndexFileStorageCASPoolString(metadata);
			Logger.info("MetadataController:getExtendedMetadata  recall Metadata File from Centera "+guid);
			String indexPoolString = Helper.getCASStorageConnectionStringbyStorage(storageD);
			CenteraAdapter indexAdapter = 
					new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
							appName, appVersion);
			return getFileFromCentera(indexAdapter, Helper.getExtendedMetadata(metadata), guid);
			}else{
				Logger.info("MetadataController:getExtendedMetadata recall Metadata File from HCP "+guid);
				S3Adapter s3adapter = new S3Adapter(storageD.getHcpS3Url(),storageD.getBucketName(),storageD.getAccessKey(),storageD.getSecretKey());
				String hcpFilename=metadata.getGuid()+Constant.HCP_INDEX_FILE_NAME_PREFIX;
				return getFileFromHCP(s3adapter,hcpFilename,guid);
			}
		}
	}

	
	public static Result downloadExtendedMetadata(String guid, String projectid) {        
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) return badRequest("Invalid parameters");

		Metadata metadata = Metadata.findByGuid(guid, projectid);

		if(metadata == null)
			return notFound("Data with " + guid +" Not Found.");
		else{
			String filename = metadata.getFileName().split("\\.")[0].concat(".indx");
			
			Storage storageD=Helper.getIndexStorageDetails(metadata);
			Logger.debug("MetadataController: storageD: "+storageD);
			Integer StorageTypeId=Helper.getStorageType(storageD);
			Logger.debug("MetadataController:downloadExtendedMetadata storageType: "+StorageTypeId);
			//check the storage type, if 1 download from centera
			if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				//String indexPoolString = Helper.getIndexFileStorageCASPoolString(metadata);
				String indexPoolString = Helper.getCASStorageConnectionStringbyStorage(storageD);
				CenteraAdapter indexAdapter = 
						new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
								appName, appVersion);
				Logger.info("MetadataController:downloadExtendedMetadata  recall Metadata File from Centera "+guid);
				return getFileFromCentera(indexAdapter, Helper.getExtendedMetadata(metadata), filename);
			}else{
				Logger.info("MetadataController:downloadExtendedMetadata  recall Metadata File from HCP "+guid);
				S3Adapter s3adapter = new S3Adapter(storageD.getHcpS3Url(),storageD.getBucketName(),storageD.getAccessKey(),storageD.getSecretKey());
				String hcpFilename=metadata.getGuid()+Constant.HCP_INDEX_FILE_NAME_PREFIX;
				return getFileFromHCP(s3adapter,hcpFilename,filename);
			}
			
			
		}
	}
	

	private static Result getExtendedMetadataFromFile(String guid, String projectid) {        
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) return badRequest("Invalid parameters");

		Metadata metadata = Metadata.findByGuid(guid, projectid);

		if(metadata == null)
			return notFound("Data with " + guid +" Not Found.");
		else{
			String uri = Helper.getExtendedMetadata(metadata);
			uri = (uri != null ? uri : "");
			if ("".equals(uri)) return notFound("no extended metadata");

			Path path = null;
			if (uri.startsWith("file://")) {
				path = Paths.get(URI.create(uri));
				if (path != null && Files.exists(path)) {
					return ok(path.toFile());   
				} else {
					return notFound();
				}
			} else  {
				return ok(uri);
			}
		}
	}

	//Before updating archive_file_master table, we will check if the record exists for the given guid
	private static ArchiveFileRegistrationVO getAIMSRegistrationInfo(CenteraClip clip)
	{
		ArchiveFileRegistrationVO responseArchiveFileRegistrationVO = null;

		try {

		} catch (Exception e) {
			e.printStackTrace();
		}
		return responseArchiveFileRegistrationVO;

	}

	private static void updateAIMSRegistrationMasterInfo(String metadataId,CenteraClip clip)
	{
		AIMSRegistration aimsInfo = null;
		try {
			//Check in AIMS_REGISTRATION Table if the metadata id exists
			aimsInfo = AIMSRegistration.findById(metadataId);
			if (aimsInfo == null || aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS)
			{
				Logger.warn("updateAIMSRegistrationMasterInfo() - Not able to retrieve AIMS guid in AIMS REGISTRATION Table for :"+metadataId +" with status=1");
				return;
			}

			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_REGISTRATION_UPDATE_INDEX_URL_KEY);
			String sourceId = AppConfigProperty.getAppConfigValue("UDAS_SOURCE_SYSTEM_ID");
			String aimsUpdateRegistrationUrl = root + extention.replace("{sourceSystem}", sourceId);

			if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
			{
				Logger.error("updateAIMSRegistrationMasterInfo() - "+Constant.AIMS_REGISTRATION_UPDATE_INDEX_URL_KEY + " not found - skipping update Index Details");
				return;
			}
			
			if(aimsInfo.getAimsGuid() == null || aimsInfo.getAimsGuid().isEmpty()) {
				Logger.warn("updateAIMSRegistrationMasterInfo() - " +
						"Bad AIMS REGISTRATION Record for :" + 
						metadataId + " unable to update AIMS Regsitration info.");
				return;
			}

			//Setting AIMS update Registration url 
			aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_PROJECT_ID_STRING, clip.projectid);
			aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_GUID_STRING, aimsInfo.getAimsGuid());

			Logger.debug("updateAIMSRegistrationMasterInfo() - aimsUpdateRegistrationUrl "+aimsUpdateRegistrationUrl);
			Logger.debug("updateAIMSRegistrationMasterInfo() - AIMS guid :"+aimsInfo.getAimsGuid()+" index size "+clip.size);

			ArchiveFileRegistrationVO archiveFileRegistrationVO = new ArchiveFileRegistrationVO();
			archiveFileRegistrationVO.setIndexFileSize(clip.size);

			
			//Making a json call to update the index file size
			ObjectMapper objectMapper = new ObjectMapper(); 
			JsonNode jsonNode = objectMapper.convertValue(archiveFileRegistrationVO, JsonNode.class);
			Logger.debug("updateAIMSRegistrationMasterInfo() - Making a jason call to AIMS");
			JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(aimsUpdateRegistrationUrl, jsonNode);

			ArchiveFileRegistrationVO responseArchiveFileRegistrationVO = null;
			if(responseJsonNode != null) {
				responseArchiveFileRegistrationVO = objectMapper.convertValue(responseJsonNode, ArchiveFileRegistrationVO.class);
				Logger.debug("updateAIMSRegistrationMasterInfo() - responseArchiveFileRegistrationVO" +responseArchiveFileRegistrationVO.getAimsGuid()+" "+responseArchiveFileRegistrationVO.getIndexFileSize());
				
				aimsInfo.setStatus(Constant.AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE);
			}else
			{
				Logger.warn("updateAIMSRegistrationMasterInfo() -Response from AIMS is null for : "+clip.guid);
				aimsInfo.setStatus(Constant.AIMS_REGISTRATION_STATUS_INDEX_REGISTRATION_FAILED);
			}
			aimsInfo.save();
		} catch (Exception e) {
			Logger.error("MetadataController - Exception occurred while " +
					"updating index file size or event based info after triggered event to AIMS", e);
			e.printStackTrace();
			try{
				aimsInfo.setStatus(Constant.AIMS_REGISTRATION_STATUS_INDEX_REGISTRATION_FAILED);
				aimsInfo.save();
			}catch(Exception ex)
			{
				Logger.error("updateAIMSRegistrationMasterInfo() - unable to update AIMS_REGISTRATION with status=4 for "+aimsInfo.getId());
				Logger.error("MetadataController - Exception occurred while " +
						"updating index file size to AIMS", ex);
				ex.printStackTrace();
			}
		}
	}

	public static Result storageUri(String guid, String projectid) { 
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) return badRequest("Invalid parameters");

		Metadata data = Metadata.findByGuid(guid, projectid);

		if(data != null) {
			String storageUri = Helper.getStorageUri(data);
			response().setHeader("x-storage-uri", storageUri);
			return ok(storageUri);
		}

		return notFound();
	}

	public static Result getWriteStorageConnectionString() { 

		return ok((String)Cache.get(
				Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY));
	}
	
	public static Result getHCPWriteStorageConnectionConfiguration() { 
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode obj =  mapper.createObjectNode();
	        obj.put(Constant.HCP_CURRENT_S3_END_URL, (String)Cache.get(Constant.HCP_CURRENT_S3_END_URL));
	        obj.put(Constant.HCP_CURRENT_REST_END_URL, (String)Cache.get(Constant.HCP_CURRENT_REST_END_URL));
	        obj.put(Constant.HCP_CURRENT_BUCKET, (String)Cache.get(Constant.HCP_CURRENT_BUCKET));
	        obj.put(Constant.HCP_CURRENT_ACCESS_KEY, (String)Cache.get(Constant.HCP_CURRENT_ACCESS_KEY));
	        obj.put(Constant.HCP_CURRENT_SECRET_KEY, (String)Cache.get(Constant.HCP_CURRENT_SECRET_KEY));
	        return ok(obj);
	}
	
	public static Result getS3ConfigurationDetails(String guid, String projectid) { 
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) 
		return badRequest("Invalid parameters");
		
		Metadata data = Metadata.findByGuid(guid, projectid);
		Storage storageD=Helper.getArchiveStorageDetails(data);
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode obj =  mapper.createObjectNode();
	        obj.put(Constant.HCP_READ_S3_END_URL, storageD.getHcpS3Url());
	        obj.put(Constant.HCP_READ_REST_END_URL,storageD.getHcpRestUrl());
	        obj.put(Constant.HCP_READ_BUCKET, storageD.getBucketName());
	        obj.put(Constant.HCP_READ_ACCESS_KEY, storageD.getAccessKey());
	        obj.put(Constant.HCP_READ_SECRET_KEY, storageD.getSecretKey());
	  
		return ok(obj);
	
	}
	
	public static Result getStorageConnectionString(String guid, String projectid) { 
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) 
			return badRequest("Invalid parameters");

		Metadata data = Metadata.findByGuid(guid, projectid);

		if(data != null) {
			String archivePoolString = Helper.getArchiveStorageCASPoolString(data);
			return ok(archivePoolString);
		}

		return notFound();
	}

	public static Result getFileName(String guid, String projectid) { 
		if(Utility.isNullOrEmpty(guid) || Utility.isNullOrEmpty(projectid)) 
			return badRequest("Invalid parameters");

		Metadata data = Metadata.findByGuid(guid, projectid);

		if (data != null) {
			return ok(data.getFileName());
		} else {
			return notFound("File Not Found");
		}
	}
	
	public static Result flagUserError(String guid, String projectid, String reason) {
		String remoteAddress = request().remoteAddress() + " - ";
		try {
			if (Utility.isNullOrEmpty(reason)) {
				return badRequest("You must supply a reason to flag an archive.");
			}
			Metadata data = Metadata.findByGuid(guid, projectid);
			data.unidirectionalStatusChange("ARCHIVED", "USER_ERROR");
			data.setExtendedStatus(reason); 
			Metadata.update(data);
			Logger.info(remoteAddress + "flagging " + data.getGuid() + " as user error.");
			return ok(Json.toJson(data));
		}
		catch (Exception e) {
			Logger.info(remoteAddress + "Could not flag archive as user error: " + e.getMessage());
			return badRequest("Could not flag archive as user error: " + e.getMessage());
		}
	}

	public static Result unflagUserError(String guid, String projectid, String reason) {
		String remoteAddress = request().remoteAddress() + " - ";
		try {
			if (Utility.isNullOrEmpty(reason)) {
				return badRequest("You must supply a reason to unflag an archive.");
			}
			Metadata data = Metadata.findByGuid(guid, projectid);
			data.unidirectionalStatusChange("USER_ERROR", "ARCHIVED");
			data.setExtendedStatus(reason); 
			Metadata.update(data);
			Logger.info(remoteAddress + "unflagging " + data.getGuid() + " as normal archive.");
			return ok(Json.toJson(data));
		}
		catch (Exception e) {
			Logger.info(remoteAddress + "Could not flag archive as user error: " + e.getMessage());
			return badRequest("Could not flag archive as user error: " + e.getMessage());
		}
	}

	public static Result extendedSearch(String guid, String projectid) {
		String result = JsonAdapter.find(JsonAdapter.TEST_DATA, "$.metadata.counterparty_historydate.last", "");
		return ok(result);
	}

	private static class NewlineFilterOutputStream extends FilterOutputStream {
		private static final byte NEWLINE = '\n';
		private static final byte NEWLINE2 = '\r';
		private static final byte TAB = '\t';
		private boolean on = false;
		public NewlineFilterOutputStream(OutputStream out) {
			super(out);
		}

		@Override
		public void write(int b) throws IOException {
			if (on) {
				if (b == NEWLINE || b == NEWLINE2) out.write(TAB);
				else super.write(b);
			} else {
				super.write(b);
			}
		}

		public void on() {
			on = true;
		}

		public void off() {
			on = false;
		}
	}


	/***
	 * Method added by Satish Kommineni
	 * This method is used for saving the Access Data.
	 * Will be called from the stream app for the login successful/unsucessful
	 */
	public static Result saveAccessData(String projectid) {
		Logger.info("inside saveAccessData method");
		String remoteAddress = request().remoteAddress() + " - ";	     	    
		JsonNode json = request().body().asJson();	   	 
		if(json == null) {
			Logger.error(remoteAddress + "Json request body is blank");
			return badRequest("400 Bad Request");
		} else {	          
			AccessData accessData = null;
			try {
				accessData = Json.fromJson(json, AccessData.class);
				accessData.save();
				Logger.info("Access Data saved.");
			} catch (Throwable e) {
				Logger.error(e.getMessage());
				return badRequest("400 Bad Request");
			}	
			return ok("SUCCESS");
		}
	} 

	public static Map<String, String> deleteArchiveForId(String id) {
		Logger.debug("MetaController:Delete In Delete archive by id: "+id);
		Map<String, String> result = new HashMap<String, String>();
		String extendedMetadataUri=null;
		String indexPoolString=null;
		try {
			String storageUri = null;            
			Metadata data = Metadata.findById(id);
			if(data != null){
				if("DELETED".equals(data.getStatus()))
				{

					Logger.info("MetaController:Delete Archive - " + data.getFileName() + " with Guid - "  +  data.getGuid() +" already been deleted. Last modified at: " + new Date(data.getModificationTimestamp()));
					result.put("code","G");
					result.put("text", Constant.ERROR_CODE_G);
				}
				else
				{
					//TD-DO
		
					Storage storageD=Helper.getArchiveStorageDetails(data);
					Logger.debug("MetadataController:deleteArchiveForId storageD: "+storageD);
					Integer StorageTypeId=Helper.getStorageType(storageD);
					Logger.debug("MetadataController:deleteArchiveForId storageType: "+StorageTypeId);
					//check the storage type, if 1 delete from centera
					
				
					int status = 1;
					
					if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
					
					storageUri = Helper.getStorageUri(data);	
					status=deleteClipsfromCentera(status,id,storageUri,extendedMetadataUri,indexPoolString,data);
						
					}// end of centera
					else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
					
					status=deleteObjectFromHCP(status,id,storageD,extendedMetadataUri,indexPoolString,data);
					
					} // end of HCP
					
					if(status >= 1)
					{
						data.setStatus("DELETED");
						data.setModifiedBy(Constant.AIMS_DISPOSITION_JOB_CYCLE);
						data.setModificationTimestamp(Utility.getCurrentTime());
						Metadata.update(data);
						result.put("code","B");
						result.put("text", "ok");
						Logger.info("MetaController:Delete Deleted archive file (" + id + ") with storageUri (" + storageUri + ") by user AIMS_DISPOSITION_JOB_CYCLE  " + id);
					}	
				} //end of else
			} //end of if where data!=null
			else
			{
				Logger.info("Archive with ID not found by AIMS_DISPOSITION_JOB_CYCLE.");
				result.put("code", "A");
				result.put("text", Constant.ERROR_CODE_A);
			}
		}
		catch (Exception exc) {
			Logger.error("Could not delete records." + exc.getMessage());
			result.put("code", "I");
			result.put("text", Constant.ERROR_CODE_I);
		}



		return result;
	}
	
	
		private static Integer deleteClipsfromCentera(int status,String id,String storageUri ,String extendedMetadataUri, String indexPoolString, Metadata data )throws Exception{
		
		Integer StorageIndexTypeId=0;	
		if(storageUri != null && storageUri.length() > 0) {
		String archivePoolString = Helper.getArchiveStorageCASPoolString(data);
		CenteraAdapter archiveAdapter = 
				new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,
						appName, appVersion);
		
				
		List<MetadataHistory> listMetaHistory = MetadataHistory.listByGuidOrderByCreateTimestamp(data.getGuid());
		Map<String, Long> deleteHistoryMap = new HashMap<String, Long>();
		
		extendedMetadataUri = Helper.getExtendedMetadata(data);
		if(extendedMetadataUri!=null)
		{
			    Storage storageIndexD=Helper.getIndexStorageDetails(data);
				Logger.debug("MetadataController: delete index : "+storageIndexD);
				StorageIndexTypeId=Helper.getStorageType(storageIndexD);
		}
		
		
		if(listMetaHistory.size()>0){
			//iterate over list
			Logger.debug("MetaController:Delete start iterating over ILM_METADATA_HISTORY "+id);
		for (MetadataHistory metaHistory : listMetaHistory) { 
			
			if(deleteHistoryMap.containsKey(metaHistory.getStorageUriPrev())){
				Logger.debug("MetaController:Skipped Clip: "+metaHistory.getStorageUriPrev());
				continue;
			}
			
			
			
				Logger.debug("MetaController:Delete metaHistory.getEbrComment()--"+metaHistory.getEbrComment()+"  id  "+id);
				if((metaHistory.getEbrComment()!=null) && (metaHistory.getEbrComment().equalsIgnoreCase(Constant.DISABLED) || metaHistory.getEbrComment().equalsIgnoreCase(Constant.EBR_TO_EBR_EXT) || metaHistory.getEbrComment().equalsIgnoreCase(Constant.EBR_TO_EBR_SHO) || metaHistory.getEbrComment().equalsIgnoreCase(Constant.EBR_TO_EBR_NO_CHANGE) || metaHistory.getEbrComment().equalsIgnoreCase(Constant.PERMOFF) || metaHistory.getEbrComment().equalsIgnoreCase(Constant.UNTRIGGERED))){
					Logger.debug("MetaController:Delete Deleting Perm archived "+id);
					//if(!deleteHistoryMap.containsKey(metaHistory.getStorageUriPrev())){
						deleteHistoryMap.put(metaHistory.getStorageUriPrev() , metaHistory.getRetentionEndPrev());
						
						Logger.debug("MetaController:Skipped history status: "+metaHistory.getStorageUriPrevStatus());
						if(metaHistory.getStorageUriPrevStatus()!=null && metaHistory.getStorageUriPrevStatus().equals(Constant.METADATA_HISTORY_STATUS_DELETED)){
							continue;
						}
						
						status = archiveAdapter.auditedDelete(metaHistory.getStorageUriPrev(),"Privileged delete issued to delete archive - as instructed by RIMS - which is under permanent retention");
						Logger.debug("MetaController:Deleted Privledge Permament archive Status: "+status+"   id  "+id);
						extendedMetadataUri = Helper.getHistoryExtendedMetadata(metaHistory);
						
						
						
						if(status == 1 && 
								extendedMetadataUri != null && 
								extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA") && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
						indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
						deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,"Privileged delete issued to delete archive index - as instructed by RIMS - which is under permanent retention");
						}
						
						if(status==-1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_FAILED);
							break;
						}else if (status==1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_DELETED);
						}
					 //}
				}else{
					Logger.debug("MetaController:Delete adding to delete map Non perm  "+id);
					//if(!deleteHistoryMap.containsKey(metaHistory.getStorageUriPrev())){
					deleteHistoryMap.put(metaHistory.getStorageUriPrev() , metaHistory.getRetentionEndPrev());
					
					Logger.debug("MetaController:Skipped history status: "+metaHistory.getStorageUriPrevStatus());
					if(metaHistory.getStorageUriPrevStatus()!=null && metaHistory.getStorageUriPrevStatus().equals(Constant.METADATA_HISTORY_STATUS_DELETED)){
						continue;
					}
					
					if(metaHistory.getRetentionEndPrev()==null || metaHistory.getRetentionEndPrev() > System.currentTimeMillis()){
						status = archiveAdapter.auditedDelete(metaHistory.getStorageUriPrev(),"Privileged delete issued to delete archive - as instructed by RIMS - which is either under permanent retention or has future retention end date");
						Logger.debug("MetaController:Delete  Privledge delete the archive status: "+status+"  id  "+ id);
						extendedMetadataUri = Helper.getHistoryExtendedMetadata(metaHistory);
						if(status == 1 && 
								extendedMetadataUri != null && 
								extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA") && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
						indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
						deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,"Privileged delete issued to delete archive index - as instructed by RIMS - which is either under permanent retention or has future retention end date");
						}
						
						if(status==-1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_FAILED);
							break;
						}else if (status==1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_DELETED);
						}
						
					}else{
						status = archiveAdapter.delete(metaHistory.getStorageUriPrev());
						Logger.debug("MetaController:Delete   Normal delete the archive status: "+status+"  id  "+id);
						extendedMetadataUri = Helper.getHistoryExtendedMetadata(metaHistory);
						if(status == 1 && 
								extendedMetadataUri != null && 
								extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA") && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
						indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
				    	deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,null);
						}
						
						if(status==-1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_FAILED);
							break;
						}else if (status==1){
							MetadataHistory.updateStorageUriStatus(data.getGuid(),metaHistory.getStorageUriPrev(),Constant.METADATA_HISTORY_STATUS_DELETED);
						}
					}
					
					Logger.debug("MetaController:Delete  deleteHistoryMap after adding ---"+deleteHistoryMap +  id);
					//}	// end of if deleteHistoryMap Check					
					
				}
		           		
		      } //end of for loop
			/**
			for (Map.Entry<String, Long> entry : deleteHistoryMap.entrySet()) {
			    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
			    
			    if(entry.getValue() > System.currentTimeMillis()){
			    	status = archiveAdapter.auditedDelete(entry.getKey(),"Privileged delete the archives");
			    	
			    }else{
			    	status = archiveAdapter.delete(entry.getKey());	
			    	
			    }
			
			}
			*/
			Logger.debug("MetaController:Delete  delete the last from ilm_metadata  "+id);
			//delete the last from ilm_metadata
			if(deleteHistoryMap.containsKey(storageUri) || status==-1){
				Logger.debug("MetaController:Delete  Already deleted through ILM_METADATA HISTORY table or some failure"+   id);
			}else{
				if(data.getRetentionEnd()==null || data.getRetentionEnd() > System.currentTimeMillis()){
			    	status = archiveAdapter.auditedDelete(storageUri,"Privileged delete issued to delete archive - as instructed by RIMS - which is either under permanent retention or has future retention end date");
			    	Logger.debug("MetaController:Delete  Delete Last ILM_METADATA priv status:   "+status+"  id  "+id);
			    	extendedMetadataUri = Helper.getExtendedMetadata(data);
			    	if(status == 1 && 
							extendedMetadataUri != null && 
							extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA") && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
			    	indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
					deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,"Privileged delete issued to delete archive index - as instructed by RIMS - which is either under permanent retention or has future retention end date");
			    	}
			    	Logger.debug("MetaController:Delete deleting archived 1  " +id);
			    }else{
			    	status = archiveAdapter.delete(storageUri);
			    	Logger.debug("MetaController:Delete  Delete Last ILM_METADATA normal status:   "+status+"  id  "+id);
			    	extendedMetadataUri = Helper.getExtendedMetadata(data);
			    	if(status == 1 && 
							extendedMetadataUri != null && 
							extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA") && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
			    	indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
					deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,null);
			    	}
			    	Logger.debug("MetaController:Delete  deleting archived 2 status   "+status+"  id  "+id);
			    }
			}							
		}else{						
	
			Logger.info("MetaController:Delete No Data in ILM_METADATA_HISTORY table, Deleting form ILM_METADATA only "+id);
			if(Constant.TRIGGERED.equalsIgnoreCase(data.getEventBased())){
				// Privileged delete
				status = archiveAdapter.auditedDelete(storageUri,"Privileged delete EBR archive for which event has been triggered and retention has been met");
				extendedMetadataUri = Helper.getExtendedMetadata(data);
				if(status == 1 && 
						extendedMetadataUri != null && 
						extendedMetadataUri.length() > 0 && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
				indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
				deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,"Privileged delete EBR archive index for which event has been triggered and retention has been met");
				}
				Logger.debug("MetaController:Delete  EBR deleteting status  "+status+"  id  "+id);
			}else{
				Logger.debug("MetaController:Delete NON EBR delete "+id);
				
			if(data.getRetentionEnd()==null || data.getRetentionEnd() > System.currentTimeMillis()){
					
					status = archiveAdapter.auditedDelete(storageUri,"Privileged delete issued to delete archive - as instructed by RIMS - which is either under permanent retention or has future retention end date");	
					Logger.debug("MetaController:Delete No EBR, Extended retention date, Delete status   "+status);
					extendedMetadataUri = Helper.getExtendedMetadata(data);
					Logger.debug("MetaController:Delete no EBR, Extended retention date, deleteting extendedMetadataUri   "+extendedMetadataUri+"  id  "+id);
					if(status == 1 && 
							extendedMetadataUri != null && 
							extendedMetadataUri.length() > 0 && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
					indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
					deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,"Privileged delete issued to delete archive index - as instructed by RIMS - which is either under permanent retention or has future retention end date");
					Logger.debug("MetaController:Delete no EBR deleting,Extended retention date indexPoolString   "+indexPoolString+"  id  "+id);
					}
				}else{
				
					status = archiveAdapter.delete(storageUri);	
					Logger.debug("MetaController:Delete no EBR , Met Retention date, deleteting status   "+status);
					extendedMetadataUri = Helper.getExtendedMetadata(data);
					Logger.debug("MetaController:Delete no EBR , Met Retention date, deleteting extendedMetadataUri   "+extendedMetadataUri+"   id  "+id);
					if(status == 1 && 
							extendedMetadataUri != null && 
							extendedMetadataUri.length() > 0 && StorageIndexTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
					indexPoolString =Helper.getIndexFileStorageCASPoolString(data);
					deleteExtendedMetadata(extendedMetadataUri,indexPoolString,status,null);
					Logger.debug("MetaController:Delete no EBR, Met Retention date,  deleting indexPoolString   "+indexPoolString +"  id  "+id);
					}
				}
				
			
				Logger.debug("MetaController:Delete after deleting extended data no history deleteting "+id);
				
			}
		}
		//end of change of code
		
		
		
		// temp comment
		/*
		if(Constant.TRIGGERED.equalsIgnoreCase(data.getEventBased())){
			// Privileged delete
			status = archiveAdapter.auditedDelete(storageUri,"Privileged delete the EBR archives for which event has been triggered and retention has met");
		}else{
			status = archiveAdapter.delete(storageUri);	
		}
		
		Logger.info("Delete Status for Guid(" + 
				data.getGuid() + "): " + status);
		String extendedMetadataUri = Helper.getExtendedMetadata(data);
		
		if(Constant.TRIGGERED.equalsIgnoreCase(data.getEventBased()) && status == 1 && 
				extendedMetadataUri != null && 
				!extendedMetadataUri.trim().isEmpty()) {
			// Privileged delete the index for EBR archives
			String indexPoolString = 
					Helper.getIndexFileStorageCASPoolString(data);
			CenteraAdapter indexAdapter = 
					new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
							appName, appVersion);
			int deleteStatus = indexAdapter.auditedDelete(extendedMetadataUri,"Privileged delete the EBR archives for which event has been triggered and retention has met");
			Logger.info("Extended Metadata Delete Status for Guid(" + 
					data.getGuid() + "): " + deleteStatus);
		}else if(status == 1 && 
				extendedMetadataUri != null && 
				!extendedMetadataUri.trim().isEmpty()) {
			String indexPoolString = 
					Helper.getIndexFileStorageCASPoolString(data);
			CenteraAdapter indexAdapter = 
					new CenteraAdapter(indexPoolString, CAS_DATE_FORMAT,
							appName, appVersion);
			int deleteStatus = indexAdapter.delete(extendedMetadataUri);
			Logger.info("Extended Metadata Delete Status for Guid(" + 
					data.getGuid() + "): " + deleteStatus);
		}
		*/
	}
		return status;
	}
	
		private static Integer deleteObjectFromHCP(int status,String id,Storage storageD,String extendedMetadataUri, String indexPoolString, Metadata data)throws Exception{
			
			String guid=data.getGuid();
			Logger.debug("Delete from HCP guid:"+ guid);
			int indexFileStatus=1;
			List<MetadataHistory> metaHistoryList = MetadataHistory.listByGuidOrderByCreateTimestamp(data.getGuid());
			HCPAdapter hcpAdapter=new HCPAdapter(storageD.getHcpRestUrl(),storageD.getAccessKey(),storageD.getSecretKey());
			S3Adapter s3adapter = new S3Adapter(storageD.getHcpS3Url(),storageD.getBucketName(),storageD.getAccessKey(),storageD.getSecretKey());
			boolean hcpPrivdelete=false;
			
			if(metaHistoryList.size()>0){
							Logger.debug("it has ILM_METADATA_HISTORY guid: "+guid);
							//check for DISABLED, EBR_TO_EBR_EXT, EBR_TO_EBR_SHO, PERMOFF or  UNTRIGGERED in update_comment column in history table
							if(MetadataHistory.checkUpdateCommentsIfAny(guid)){
								Logger.debug("Permanent delete archive guid: "+guid);	
								status=hcpAdapter.privdelete(guid,"Privileged_delete_issued_to_delete_archive_as_instructed_by_RIMS_which_is_under_permanent_retention");
								hcpPrivdelete=true;
							}else if(MetadataHistory.checkNullRetentionEndDate(guid)){
								 Logger.debug("check for retention end date prev is null in Metadata History table guid: "+guid);	
							     status=hcpAdapter.privdelete(guid,"Privileged_delete_issued_to_delete_archive_as_instructed_by_RIMS_which_is_under_permanent_retention");
							     hcpPrivdelete=true;
							}else if(data.getRetentionEnd()==null){
								 Logger.debug("check for retention end date  is null in Metadata table guid: "+guid);	
						     	 status=hcpAdapter.privdelete(guid,"Privileged_delete_issued_to_delete_archive_as_instructed_by_RIMS_which_is_under_permanent_retention");
						     	 hcpPrivdelete=true;
							}else{
								Logger.debug("comparing retention end date with history retention end date guid: "+guid);	
								Long maxRetenDateHistory=MetadataHistory.getMaxRetentionEndDate(guid);
								if(data.getRetentionEnd()>maxRetenDateHistory){
									maxRetenDateHistory=data.getRetentionEnd();
								}
								Logger.debug("comparing retention end date with history retention end date maxRetenDateHistory "+maxRetenDateHistory+"  guid: "+guid);	
								if(maxRetenDateHistory > System.currentTimeMillis()){
								   status=hcpAdapter.privdelete(guid,"Privileged_delete_issued_to_delete_archive_as_instructed_by_RIMS_which_has_future_retention_end_date");
								   hcpPrivdelete=true;
								}
								else{
								   status=s3adapter.delete(guid);
								}
								
							}
							
						}else{
							Logger.debug("it doesnot have ILM_METADATA_HISTORY guid: "+guid);
							if(Constant.TRIGGERED.equalsIgnoreCase(data.getEventBased())){
								//hcp
								Logger.debug("Privleged delete the TRIGGERED archived guid: "+guid);
								status=hcpAdapter.privdelete(guid,"Privileged_delete_EBR_archive_index_for_which_event_has_been_triggered_and_retention_has_been_met");
								hcpPrivdelete=true;
							}else{
								if(data.getRetentionEnd()==null || data.getRetentionEnd() > System.currentTimeMillis()){
									Logger.debug("Privleged delete the permanent or future archived guid: "+guid);
									status=hcpAdapter.privdelete(guid,"Privileged_delete_issued_to_delete_archive_as_instructed_by_RIMS_which_is_either_under_permanent_retention_or_has_future_retention_end_date");
									hcpPrivdelete=true;
								}else{
								Logger.debug("Normal delete the archived guid: "+guid);
								status=s3adapter.delete(guid);
								}
							}
							
						}
			
			extendedMetadataUri = Helper.getExtendedMetadata(data);
			if(status == 1 && 
					extendedMetadataUri != null && 
					extendedMetadataUri.length() > 0 && !extendedMetadataUri.equalsIgnoreCase("NA")){
				
					Storage storageIndexD=Helper.getIndexStorageDetails(data);
					Logger.debug("MetadataController: delete index : "+storageIndexD+"   guid: "+guid);
					Integer StorageIndexTypeId=Helper.getStorageType(storageIndexD);
					Logger.debug("MetadataController:downloadExtendedMetadata storageType: "+StorageIndexTypeId+"guid: "+guid);
					HCPAdapter indexHcpAdapter=new HCPAdapter(storageIndexD.getHcpRestUrl(),storageIndexD.getAccessKey(),storageIndexD.getSecretKey());
					S3Adapter indexS3adapter = new S3Adapter(storageIndexD.getHcpS3Url(),storageIndexD.getBucketName(),storageIndexD.getAccessKey(),storageIndexD.getSecretKey());
					if(StorageIndexTypeId==Constant.STORAGE_TYPE_HCP_ID){
						if(hcpPrivdelete==true){
							 indexFileStatus= indexHcpAdapter.privdelete(extendedMetadataUri,"Privileged_delete_issued_to_delete_archive_index_as_instructed_by_RIMS_which_is_either_under_permanent_retention_or_has_future_retention_end_date");
							 Logger.info("indexFileStatus :guid: "+guid+"-"+indexFileStatus);
						 }else{
							 indexFileStatus= indexS3adapter.delete(extendedMetadataUri);
							 Logger.info("indexFileStatus guid: "+guid+" - "+indexFileStatus);
						 }
					}
					
			}
			
			Logger.debug("HCP delete Satus: guid: "+guid+" : "+status);
			return status;
		}
}

