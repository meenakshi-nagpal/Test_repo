package jobs.aims;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jobs.util.EmailSender;
import jobs.util.UpdateProcessUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import models.storageapp.AIMSRegistration;
import models.storageapp.AppConfigProperty;
import models.storageapp.Metadata;
import models.storageapp.MetadataAnalysis;
import models.storageapp.MetadataHistory;
import models.storageapp.RetentionCycle;
import models.storageapp.RetentionUpdates;
import play.Logger;
import utilities.Constant;
import utilities.Guid;
import valueobjects.RecordCodeVO;
import ws.AIMSWSClient;

public class ApplyRetentionUpdates {


	
	static String applyRetentionUpdates(String executionMode, String ait, String projectId, String recordCode) throws Exception{
		Logger.info("Entering applyRetentionUpdates()");
		long startTime = System.currentTimeMillis();
		int maxHours =0;
		long maxHoursInMiliSec = 0L;
		AppConfigProperty appConfigProperty1 = AppConfigProperty.getPropertyByKey(Constant.APPLY_RETENTION_MAX_RUN_HOURS);
			if(appConfigProperty1 == null || appConfigProperty1.getValue() == null || appConfigProperty1.getValue().trim().isEmpty()) {
			Logger.warn("ApplyRetentionUpdatesJob - "+Constant.APPLY_RETENTION_MAX_RUN_HOURS + " not found, running without an upper time limit");
		}else{
			maxHours = Integer.parseInt(appConfigProperty1.getValue());
			maxHoursInMiliSec = maxHours * 3600L * 1000L;
		}
			
		List<MetadataAnalysis> analysisData = new ArrayList<MetadataAnalysis>();
		List<MetadataHistory> historyData = new ArrayList<MetadataHistory>();
		Map<String,MetadataAnalysis> toBeUpdatedAnalysis = Collections.synchronizedMap(new HashMap<String,MetadataAnalysis>());//new HashMap<String, MetadataAnalysis>();//Collections.synchronizedMap(new HashMap<String,MetadataAnalysis>());
		int status=0;
		boolean checkIfAllUpdated = false;
		int batchCount = 0;
		AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.APPLY_RETENTION_BATCH_COUNT);
			if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
			Logger.warn("ApplyRetentionUpdatesJob - "+Constant.APPLY_RETENTION_BATCH_COUNT + " not found, running for batch coutn = 10000");
			batchCount =10000;
		}else{
			batchCount = Integer.parseInt(appConfigProperty.getValue());
		}
		int count = 0;
		int batchCounter = 0;
		
		RetentionCycle currentRetentionCycle = UpdateProcessUtil.getCurrentRetentionCycle();
		if(currentRetentionCycle == null){
			Logger.info("No Cycle available to run the updates for");
			return ("No Cycle available to run the updates for");
		}
		
		Integer cycleId = currentRetentionCycle.getCycleId();
		Integer currentStatus = currentRetentionCycle.getCurrentStatus();
		String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_RUN_MODE);
		if(!UpdateProcessUtil.analysisAlreadyValidated(currentStatus)){
			//no more analysis can be performed
			Logger.info("Full Analysis is not validated, updates cannot be performed");
			return ("Full Analysis is not validated, updates cannot be performed");
		}else if(UpdateProcessUtil.updatesFullyCompleted(currentStatus)){
			//All records have been updated
			Logger.info("All records have been updated for this cycle");
			Logger.info("checking if aims confirmation sent..");
			List<RetentionUpdates> rt = RetentionUpdates.getAllConfirmationNotSent();
			if(!rt.isEmpty()){
				Logger.info("sending pending aims confirmation..");
				boolean confirmationSent = sendConfirmationToAims(null, rt);
				if(confirmationSent){
					Logger.info("Confirmation sent to aims, updating cycle status to updates confirmed");
					status = Constant.CYCLE_STATUS_UPDATES_CONFIRMED;
					UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
					UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
					
					Logger.info("Updating and inserting retention cycle status to completed");
					status = Constant.CYCLE_STATUS_CYCLE_COMPLETED;
					UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
					UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
				} 
				if(status == Constant.CYCLE_STATUS_CYCLE_COMPLETED){
					sendEmail("update","");
				}
			}
						
			return ("All records have been updated for this cycle");
		}
		
		
		
		Logger.info("Updating and inserting retention cycle status to in progress");
		status = Constant.CYCLE_STATUS_UPDATES_INPROGRESS;
		UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
		UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
		
		if(runForAll(ait, projectId, recordCode)){
			Logger.info("Running updates for all record available in ILM_METADATA_ANALYSIS table");
			analysisData = MetadataAnalysis.allWithDBUpdatedNo();
			if(analysisData.isEmpty()){
				Logger.info("All guids in ILM_METADATA_ANALYSIS table are already processed");
				return ("All guids in ILM_METADATA_ANALYSIS table are already processed");
			}
			Logger.info("applying updates on "+analysisData.size()+" records");
			status = Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED;
		}else{
			Logger.info("Running updates for given parameters : ait="+ait+" projectid="+projectId+" recordCode="+recordCode);
			analysisData = getAnalysisForParameters(ait, projectId, recordCode);
			if(analysisData.isEmpty()){
				Logger.info("No guids found in ILM_METADATA_ANALYSIS table for given parameters");
				return ("No guids found in ILM_METADATA_ANALYSIS table for given parameters");
			}
			Logger.info("applying updates on "+analysisData.size()+" records");
			status = Constant.CYCLE_STATUS_UPDATES_PARTIAL_COMPLETED;
			checkIfAllUpdated = true;
		}
		//prepare METADATA_HSTORY
		for(MetadataAnalysis analysis : analysisData){
			
			try{
				MetadataHistory historyMetadata = createMetadataHistory(analysis);
				historyData.add(historyMetadata);
				toBeUpdatedAnalysis.put(analysis.getGuid()+","+analysis.getCycleId(),analysis);
				count++;
			}catch(Exception e){
				Logger.error("Error occurred while creating history for guid "+analysis.getGuid()+" cycleId"+analysis.getCycleId()+" "+e);
			}
			if(count == batchCount){
				batchCounter++;
				Logger.info("Processing batch "+batchCounter);
				List<Metadata> updatedMetadata = Collections.synchronizedList(new ArrayList<Metadata>());//new ArrayList<Metadata>();  
				List<MetadataHistory> updatedHistoryData = Collections.synchronizedList(new ArrayList<MetadataHistory>());//new ArrayList<MetadataHistory>();
				List<MetadataAnalysis> updatedAnalysisData = Collections.synchronizedList(new ArrayList<MetadataAnalysis>());//new ArrayList<MetadataAnalysis>();
				List<String> metadataIds = Collections.synchronizedList(new ArrayList<String>());//new ArrayList<String>();
	
				ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				for(MetadataHistory history : historyData){
					//applyUpdates(history,updatedMetadata,updatedHistoryData,updatedAnalysisData,toBeUpdatedAnalysis,metadataIds);
					
					executor.execute(new ApplyUpdates(history,updatedMetadata,updatedHistoryData,updatedAnalysisData,toBeUpdatedAnalysis,metadataIds));
				}
				executor.shutdown();
			    while (!executor.isTerminated()) {
			    }
			    
			    try{
					Logger.info("Inserting "+updatedHistoryData.size()+" records in ILM_METADATA_HISTORY");
					//MetadataHistory.saveAll(updatedHistoryData);
					MetadataHistory.insertAll(updatedHistoryData);
					Logger.info("Updating "+updatedMetadata.size()+" records in ILM_METADATA");
					//Metadata.saveAll(updatedMetadata);
					Metadata.updateAll(updatedMetadata);
					Logger.info("Updating DB flag for "+updatedAnalysisData.size()+" records in ILM_METADATA_ANALYSIS");
					//MetadataAnalysis.saveAll(updatedAnalysisData);
					MetadataAnalysis.updateAll(updatedAnalysisData);
					
					if(!metadataIds.isEmpty()){
						//update status in AIMS REGISTRATION TABLE to EVENT_BASED_UPDATE_PENDING
						updateAIMSRegistrationstatus(metadataIds);
					}
			    }catch(Exception e){
			    	Logger.error("Error occurred while writing batch "+batchCounter +" to database");
			    }
			    
				updatedMetadata = null;
				updatedHistoryData = null;
				updatedAnalysisData = null;
				metadataIds = null;
				//historyData.removeAll(historyData);
				historyData = null;
				toBeUpdatedAnalysis = null;
				count=0;
				historyData = new ArrayList<MetadataHistory>();
				toBeUpdatedAnalysis = new HashMap<String, MetadataAnalysis>();
				
				if(maxHoursInMiliSec > 0L){
					long currentTime = System.currentTimeMillis();
					if(currentTime - startTime >= maxHoursInMiliSec){
						Logger.info("Max time limit reached...exiting");
						
						if(UpdateProcessUtil.allUpdated()){
							status = Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED;
						}else{
							status = Constant.CYCLE_STATUS_UPDATES_PARTIAL_COMPLETED;
						}
						Logger.info("Updating and inserting retention cycle status");
						UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
						UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
						
						if(currentRetentionCycle.getCurrentStatus() == Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED){
							//send confirmation to aims
							Logger.info("sending confirmation to aims");
							boolean confirmationSent = sendConfirmationToAims(currentRetentionCycle.getCycleId(),null);
							if(confirmationSent){
								Logger.info("Confirmation sent to aims, updating cycle status to updates confirmed");
								status = Constant.CYCLE_STATUS_UPDATES_CONFIRMED;
								UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
								UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
								
								Logger.info("Updating and inserting retention cycle status to completed");
								status = Constant.CYCLE_STATUS_CYCLE_COMPLETED;
								UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
								UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
							} 
						}
						if(status == Constant.CYCLE_STATUS_CYCLE_COMPLETED){
							sendEmail("update","");
						}
						
						Logger.info("Exiting applyRetentionUpdates()");
						return ("Max time limit reached...exiting");
					}
				}
			}
		}
		
		//for remaining items
		if(!historyData.isEmpty()){
			batchCounter++;
			Logger.info("Processing batch "+batchCounter);
			List<Metadata> updatedMetadata = Collections.synchronizedList(new ArrayList<Metadata>());//new ArrayList<Metadata>();  
			List<MetadataHistory> updatedHistoryData = Collections.synchronizedList(new ArrayList<MetadataHistory>());//new ArrayList<MetadataHistory>();
			List<MetadataAnalysis> updatedAnalysisData = Collections.synchronizedList(new ArrayList<MetadataAnalysis>());//new ArrayList<MetadataAnalysis>();
			List<String> metadataIds = Collections.synchronizedList(new ArrayList<String>());//new ArrayList<String>();
			
			
			ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			for(MetadataHistory history : historyData){
				//applyUpdates(history,updatedMetadata,updatedHistoryData,updatedAnalysisData,toBeUpdatedAnalysis,metadataIds);
				
				executor.execute(new ApplyUpdates(history,updatedMetadata,updatedHistoryData,updatedAnalysisData,toBeUpdatedAnalysis,metadataIds));
			}
			executor.shutdown();
		    while (!executor.isTerminated()) {
		    }
		    try{
				Logger.info("Inserting "+updatedHistoryData.size()+" records in ILM_METADATA_HISTORY");
				//MetadataHistory.saveAll(updatedHistoryData);
				MetadataHistory.insertAll(updatedHistoryData);
				Logger.info("Updating "+updatedMetadata.size()+" records in ILM_METADATA");
				//Metadata.saveAll(updatedMetadata);
				Metadata.updateAll(updatedMetadata);
				Logger.info("Updating DB flag for "+updatedAnalysisData.size()+" records in ILM_METADATA_ANALYSIS");
				//MetadataAnalysis.saveAll(updatedAnalysisData);
				MetadataAnalysis.updateAll(updatedAnalysisData);
	
				if(!metadataIds.isEmpty()){
					//update status in AIMS REGISTRATION TABLE to EVENT_BASED_UPDATE_PENDING
					updateAIMSRegistrationstatus(metadataIds);
			}
		    }catch(Exception e){
		    	Logger.error("Error occurred while writing batch "+batchCounter +" to database");
		    }
		}
		
		Logger.info("Updating and inserting retention cycle status");
		UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
		UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
		
		if(checkIfAllUpdated && UpdateProcessUtil.allUpdated()){
			Logger.info("All records updated changing status to UPDATES_FULL_COMPLETED ");
			status = Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED;
			UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
			UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
		}
		
		if(currentRetentionCycle.getCurrentStatus() == Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED){
			//send confirmation to aims
			Logger.info("sending confirmation to aims");
			boolean confirmationSent = sendConfirmationToAims(currentRetentionCycle.getCycleId(),null);
			if(confirmationSent){
				Logger.info("Confirmation sent to aims, updating cycle status to updates confirmed");
				status = Constant.CYCLE_STATUS_UPDATES_CONFIRMED;
				UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
				UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
				
				Logger.info("Updating and inserting retention cycle status to completed");
				status = Constant.CYCLE_STATUS_CYCLE_COMPLETED;
				UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode, analysisValidationMode);
				UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode, analysisValidationMode);
			} 
		}
		
		if(status == Constant.CYCLE_STATUS_CYCLE_COMPLETED){
			sendEmail("update","");
		}
		Logger.info("Exiting applyRetentionUpdates()");
		return "";
	}
	
	private static MetadataHistory createMetadataHistory(MetadataAnalysis analysis) {
		MetadataHistory historyMetadata = new MetadataHistory();
		historyMetadata.setId(Guid.guid());
		historyMetadata.setCycleId(analysis.getCycleId());
		historyMetadata.setGuid(analysis.getGuid());
		historyMetadata.setProjectId(analysis.getProjectId());
		historyMetadata.setAit(analysis.getAit());
		historyMetadata.setRecordCodePrev(null);
		historyMetadata.setCountryPrev(null);
		historyMetadata.setRecordCodeNew(analysis.getRecordCodeNew());
		historyMetadata.setCountryNew(analysis.getCountryNew());
		historyMetadata.setRetentionEndPrev(analysis.getRetentionEndPrev());
		historyMetadata.setRetentionEndNew(analysis.getRetentionEndNew());
		historyMetadata.setStorageUriPrev(analysis.getStorageUriPrev());
		historyMetadata.setStorageUriNew(analysis.getStorageUriNew());
		historyMetadata.setIndexFileUriPrev(analysis.getIndexFileUriPrev());
		historyMetadata.setIndexFileUriNew(analysis.getIndexFileUriNew());
		historyMetadata.setRetentionTypePrev(analysis.getRetentionTypePrev());
		historyMetadata.setRetentionTypeNew(analysis.getRetentionTypeNew());
		historyMetadata.setDbUpdateFlag(analysis.getDbUpdateFlag());
		historyMetadata.setChangeType(analysis.getChangeType());
		historyMetadata.setEbrComment(analysis.getEbrComment());
		historyMetadata.setUpdateType(analysis.getUpdateType());
		if("ER".equalsIgnoreCase(historyMetadata.getStorageUriNew()) || "R".equalsIgnoreCase(historyMetadata.getStorageUriNew())){
			historyMetadata.setExtensionStatus("SUCCESS");
		}else{
			historyMetadata.setExtensionStatus("");
		}
		
		return historyMetadata;
	}

	
	
	private static boolean sendConfirmationToAims(Integer cycleId, List<RetentionUpdates> updates) {
		if(updates == null){
			updates = RetentionUpdates.listByCycleId(cycleId);
		}
		//List<RetentionUpdates> updates = RetentionUpdates.listByCycleId(cycleId);
		List<RetentionUpdates> updatedUpdates = new ArrayList<RetentionUpdates>();
		
		for(RetentionUpdates update : updates){
			Integer aimsId = update.getAimsId();
			String url = aimsConfirmationUrl(aimsId);
			Logger.debug("ApplyRetentionUpdatesJob AIMS confirmation Url "+url);
								
			ObjectMapper objectMapper = new ObjectMapper(); 
			RecordCodeVO recordCodeVO = new RecordCodeVO();
			JsonNode jsonNode = objectMapper.convertValue(recordCodeVO, JsonNode.class);
			JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(url, jsonNode);
				
			if(responseJsonNode!=null){
				RecordCodeVO recordVOUpdate = objectMapper.convertValue(responseJsonNode, new TypeReference<RecordCodeVO>() {});
				Logger.debug("ApplyRetentionUpdatesJob  responseJsonNode- "+recordVOUpdate.getStatus()+"-");
				if("SUCCESS".equalsIgnoreCase(recordVOUpdate.getStatus())){
						//return true;
					update.setIsAimsConfirmationNotified("Y");
					updatedUpdates.add(update);
				}
				else{
					update.setIsAimsConfirmationNotified("N");
					}
			}
			else{
				update.setIsAimsConfirmationNotified("N");
			}
		}
		if(updatedUpdates.isEmpty()){
			return false;
		}else{
			RetentionUpdates.saveAll(updatedUpdates);
			return true;
		}
	}


	private static String aimsConfirmationUrl(Integer id) {
		String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		String sourceSystemId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
		String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_RETENTION_UPDATE_CONFIRMATION_URL); 

		if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
			Logger.error("ApplyRetentionUpdatesJob AIMS confirmation Url not found in App Config");
			return null;
		}

		String url = root + (extention.replace(Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
		url = (url.replace(Constant.AIMS_URL_AIMS_ID_STRING,Integer.toString(id)));
		return url;
	}

	private static boolean runForAll(String ait, String projectId, String recordCode){
		if("all".equalsIgnoreCase(ait) && "all".equalsIgnoreCase(projectId) && "all".equalsIgnoreCase(recordCode)){
			return true;
		}
		return false;
	}
	
	private static void updateAIMSRegistrationstatus(List<String> metadataIds) {
		for(String metadataId : metadataIds){
			AIMSRegistration aimsInfo = null;
			//Check in AIMS_REGISTRATION Table if the metadata id exists
			aimsInfo = AIMSRegistration.findById(metadataId);
			if (aimsInfo == null || (aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS 
					&& aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE && aimsInfo.getStatus() != Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_SUCCESS))
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
		
				
	}
	
	private static List<MetadataAnalysis> getAnalysisForParameters(String ait, String projectId, String recordCode){
		if(ait != null && projectId != null && recordCode != null){
			return MetadataAnalysis.listByAitAndProjectidAndRecordCode(ait, projectId, recordCode);
		}else if(ait != null && projectId != null && recordCode == null){
			return MetadataAnalysis.listByAitAndProjectid(ait, projectId);
		}else if(ait != null && projectId == null && recordCode != null){
			return MetadataAnalysis.listByAitAndRecordCode(ait, recordCode);
		}else if(ait == null && projectId != null && recordCode != null){
			return MetadataAnalysis.listByProjectidAndRecordCode(projectId, recordCode);
		}else if(ait != null && projectId == null && recordCode == null){
			return MetadataAnalysis.listByAit(ait);
		}else if(ait == null && projectId != null && recordCode == null){
			return MetadataAnalysis.listByProjectId(projectId);
		}else if(ait == null && projectId == null && recordCode != null){
			return MetadataAnalysis.listByRecordCode(recordCode);
		}
		
		return null;
	}

	private static void sendEmail(String mode, String runMode) throws Exception{
		String emailTo =  "";
		String emailFrom = "";
		String emailSubject = "";
		String emailBody = "";
		String reportLink = "";
		String validateLink = "";
		
		emailTo = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_EMAIL_FROM);
		emailFrom = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_EMAIL_FROM);
		emailSubject = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_EMAIL_SUBJECT);
		if(emailSubject.equalsIgnoreCase("")){
			emailSubject = "UDAS Retention Schedule Updates";
		}
		reportLink = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_EMAIL_REPORT_LINK);
		validateLink = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_EMAIL_VALIDATE_LINK);
   		
   		if("analysis".equalsIgnoreCase(mode)){
   			emailSubject = emailSubject + " - Analysis Complete";
   			
   			if(Constant.MANUAL.equalsIgnoreCase(runMode)){
   	   			emailBody = "<HTML><P>UDAS Retention Schedule Updates Analysis is now available.</P>"
   	   	   				+ "<P>Please review (<a href=\""+reportLink+"\">Analysis Summary Report</a>)</P>"
   	   	   				+"<br>"
   	   	   				+ "<P>Once review is complete, please validate the Analysis so that next Phase can proceed to apply the necessary Updates</P>"
   	   	   				+ "<P>To validate the analysis, login to <a href=\""+validateLink+"\">admin console</a> on AIMS UI.</P></HTML>";
   			}else if(Constant.AUTO.equalsIgnoreCase(runMode)){
   				emailBody = "<HTML><P>UDAS Retention Schedule Updates Analysis is now available.</P>"
   	   	   				+ "<P>Please review (<a href=\""+reportLink+"\">Analysis Summary Report</a>)</P>"
   	   	   				+"<br>"
   	   	   				+ "<P>Proceeding to apply the updates.</P></HTML>";
   			}

   		}else if("update".equalsIgnoreCase(mode)){
   			emailSubject = emailSubject + " - Updates Complete";
   			emailBody = "<HTML><P>UDAS Retention Schedule Updates have been successfully applied.</P></HTML>";
   		}

   		if(emailTo != null && !emailTo.equals("") && emailFrom != null && !emailFrom.equals("")){
   			EmailSender.sendEmail(emailTo, emailFrom, emailSubject, emailBody);
   		}else{
   			Logger.warn("emailTo and emailFrom required for sending email");
   		}
	}
	
}
