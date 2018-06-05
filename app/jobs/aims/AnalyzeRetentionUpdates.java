package jobs.aims;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jobs.util.EmailSender;
import jobs.util.UpdateProcessUtil;
import models.storageapp.AppConfigProperty;
import models.storageapp.Helper;
import models.storageapp.Metadata;
import models.storageapp.MetadataAnalysis;
import models.storageapp.MetadataHistory;
import models.storageapp.RetentionCycle;
import models.storageapp.RetentionUpdates;
import models.storageapp.Helper.TimeUnit;
import play.Logger;
import utilities.Constant;
import utilities.Guid;

class AnalyzeRetentionUpdates {
	
	private static Map<String, RetentionUpdates> retentionUpdatePerRecordCodeAndCountryCode;
	private static Map<Integer, Integer> storageMasterMap;
	
		static String analyzeRetentionUpdates(String executionMode, String ait, String projectId, String recordCode) throws Exception{
	
		Logger.info("Entering analyzeRetentionUpdates()");
		List<RetentionUpdates> retentionUpdates = new ArrayList<RetentionUpdates>();
		
		List<Metadata> impactedData = new ArrayList<Metadata>();
		List<MetadataAnalysis> analysisData = new ArrayList<MetadataAnalysis>();
		Map<String, List<MetadataHistory>> historyByGuid = null;
		int status;
		int batchCount =0;
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
			Logger.info("No Cycle available to run the analysis for");
			return ("No Cycle available to run the analysis for");
		}
		
		Integer cycleId = currentRetentionCycle.getCycleId();
		Integer currentStatus = currentRetentionCycle.getCurrentStatus();
	
		if(UpdateProcessUtil.analysisAlreadyValidated(currentStatus)){
			//no more analysis can be performed
			Logger.info("Full Analysis is already validated, no more analysis can be performed");
			return ("Full Analysis is already validated, no more analysis can be performed");
		}
		//truncate ILM_METADATA_ANALYSIS table
		MetadataAnalysis.truncate();
		Logger.info("ILM_METADATA_ANALYSIS table truncated");
		
		String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_RUN_MODE);
		
		Logger.info("Updating and inserting retention cycle status to in progress");
		status = Constant.CYCLE_STATUS_ANALYSIS_INPROGRESS;
		UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status, executionMode, analysisValidationMode);
		UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status, executionMode, analysisValidationMode);
		
		//get all retention update for current cycle
		retentionUpdates = getAllRetentionUpdatesForCycle(cycleId);
		if(retentionUpdates == null){
			Logger.info("No retention update schedule available for cycleId "+cycleId);
			return ("No retention update schedule available for cycleId "+cycleId);
		}
		
		//initialize retention_updates map
		prepareRetentionUpdatesMap(retentionUpdates);
		
		if(runForAll(ait, projectId, recordCode)){
			Logger.info("Running analysis for all record codes available in RETENTION_UPDATES table for current cycle");
	
			impactedData = Metadata.getImpactedData(cycleId);
			//impactedData = getImpactedMetadata(Metadata.listByRecordCodes(recordCodes));
			if(impactedData.isEmpty()){
				Logger.info("No metadata impacted by given retention updates in current cycle");
				return ("No metadata impacted by given retention updates in current cycle");
			}
			Logger.info("No of ILM_METADATA records impacted= "+impactedData.size());
			
			try{
				historyByGuid = getMetdataHistoryForImpactedData(cycleId,ait, projectId,recordCode, true);
			}catch(Exception e1){
				Logger.error("Error occurred getting history for impacted guids "+e1);
				e1.printStackTrace();
				throw new Exception(e1);
			}
			Logger.info("No of ILM_METADATA_HISTORY records for all impacted guids= "+historyByGuid.size());
			status = Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED;
		}else{
			Logger.info("Running analysis for given parameters : ait="+ait+" projectid="+projectId+" recordCode="+recordCode);
			impactedData = getImpactedDataForParameters(cycleId,ait,projectId, recordCode);
			if(impactedData.isEmpty()){
				Logger.info("No metadata impacted by given retention updates in current cycle for given parameters");
				return ("No metadata impacted by given retention updates in current cycle for given parameters");
			}
			Logger.info("No of ILM_METADATA records impacted= "+impactedData.size());
			
			try{
				historyByGuid = getMetdataHistoryForImpactedData(cycleId, ait, projectId, recordCode, false);
			}catch(Exception e1){
				Logger.error("Error occurred getting history for impacted guids "+e1);
				e1.printStackTrace();
				throw new Exception(e1);
			}
			Logger.info("No of ILM_METADATA_HISTORY records for all impacted guids= "+historyByGuid.size());
			status = Constant.CYCLE_STATUS_ANALYSIS_PARTIAL_COMPLETED;
		}
		
		//get storage id and storage type mapping from STORAGE_MASTER
		storageMasterMap = UpdateProcessUtil.getStorageTypeAndIdMapping();
		
		for(Metadata metadata : impactedData){
			try{
				if(!retentionTypeChanged(metadata)){
					if(Constant.ENABLED.equalsIgnoreCase(metadata.getEventBased())){
						Logger.info("metadata is in EBR enabled state, record code update is not applicable : guid "+metadata.getGuid());	
						continue;
					}else if(retentionTypePermanentRetained(metadata)){
						Logger.info("Retention type is still Permanent, no updates required : guid "+metadata.getGuid());
						continue;
					}	
					
				}
				MetadataAnalysis analysisMetadata = analyzeMetadata(metadata, cycleId, historyByGuid);
				analysisData.add(analysisMetadata);
				count++;
			}catch(Exception e){
				Logger.error("Error occurred while analyzing guid "+metadata.getGuid()+" "+e);
				e.printStackTrace();
			}
			if(count == batchCount){
				Logger.info("Processing batch "+batchCounter);
				batchCounter++;
				if(!analysisData.isEmpty()){
					try{
						Logger.info("Inserting "+analysisData.size()+" records in ILM_METADATA_ANALYSIS");
						//MetadataAnalysis.saveAll(analysisData);
						MetadataAnalysis.insertAll(analysisData);
						Logger.info("Inserted "+analysisData.size()+" records in ILM_METADATA_ANALYSIS");
					}catch(Exception e){
						Logger.error("Error occurred while writing batch "+batchCounter+" to database");
					}
				}
				count=0;
				analysisData = null;
				analysisData = new ArrayList<MetadataAnalysis>();
			}
		}
		//for remaining items
		if(!analysisData.isEmpty()){
			try{
				Logger.info("Processing batch "+batchCounter);
				Logger.info("Inserting "+analysisData.size()+" records in ILM_METADATA_ANALYSIS");
				//MetadataAnalysis.saveAll(analysisData);
				MetadataAnalysis.insertAll(analysisData);
				Logger.info("Inserted "+analysisData.size()+" records in ILM_METADATA_ANALYSIS");
			}catch(Exception e){
				Logger.error("Error occurred while writing batch "+batchCounter+" to database");
			}
		}
		
		Logger.info("Updating and inserting retention cycle status");
		UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode,analysisValidationMode);
		UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode,analysisValidationMode);
		
		if(status == Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED){
			String runMode = null;
			appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.APPLY_RETENTION_RUN_MODE);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
				Logger.warn("ApplyRetentionUpdatesJob - "+Constant.APPLY_RETENTION_RUN_MODE + " not found, running in manual run mode");
			}else{
				runMode = appConfigProperty.getValue();
			}
	   		sendEmail("analysis", runMode);
	   		
			if(Constant.AUTO.equalsIgnoreCase(runMode)){
				status = Constant.CYCLE_STATUS_ANALYSIS_VALIDATED;
				Logger.info("Updating and inserting retention cycle status - analysis validated in auto run mode");
				UpdateProcessUtil.updateRetentionCycleStatus(currentRetentionCycle,status,executionMode,analysisValidationMode);
				UpdateProcessUtil.insertInRetentionCycleHistory(cycleId,status,executionMode,analysisValidationMode);
				ApplyRetentionUpdates.applyRetentionUpdates(executionMode,ait, projectId, recordCode);
			}
			
		}
		
		Logger.info("Exiting analyzeRetentionUpdates()");
		return "";
	}
	


	private static MetadataAnalysis analyzeMetadata(Metadata metadata, Integer cycleId, Map<String, List<MetadataHistory>> historyByGuid) throws Exception {
		MetadataAnalysis analysisMetadata = new MetadataAnalysis();
		analysisMetadata.setUpdateType(Constant.RETENTION_SCHEDULE_AUTOMATION);
		analysisMetadata.setId(Guid.guid());
		analysisMetadata.setCycleId(cycleId);
		analysisMetadata.setGuid(metadata.getGuid());
		analysisMetadata.setProjectId(metadata.getProjectId());
		analysisMetadata.setAit(metadata.getAit());
		analysisMetadata.setRecordCodePrev(null);
		analysisMetadata.setCountryPrev(null);
		analysisMetadata.setRecordCodeNew(metadata.getRecordCode());
		analysisMetadata.setCountryNew(metadata.getCountry());
		analysisMetadata.setRetentionEndPrev(metadata.getRetentionEnd());
		analysisMetadata.setRetentionEndNew(getNewRetentionEnd(metadata));
		
		Long retentionEndPrev = analysisMetadata.getRetentionEndPrev();
		Long retentionEndNew = analysisMetadata.getRetentionEndNew();
		if(analysisMetadata.getRetentionEndPrev() == null){
			retentionEndPrev = Constant.MAX_DATE; //253399640400000L;  //max date : 12/01/9999
		}
		if(analysisMetadata.getRetentionEndNew() == null){
			retentionEndNew = Constant.MAX_DATE; //253399640400000L;  //max date : 12/01/9999
		}
					
		analysisMetadata.setStorageUriPrev(Helper.getStorageUri(metadata));
		
		//Storage archiveStorage = Storage.findById(Helper.getArchiveStorageId(metadata));
		
		Integer archiveStorageType = storageMasterMap.get(Helper.getArchiveStorageId(metadata));
		if(archiveStorageType == null || archiveStorageType == 0){
			Logger.warn("No storage type found for this archive storage id.");
			throw new Exception("No storage type found for this archive storage id");
		}
		if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
			if(UpdateProcessUtil.isNewClipOrHCPExtensionRequired(metadata, retentionEndNew, historyByGuid)){
				analysisMetadata.setStorageUriNew("ER");
			}else{
				analysisMetadata.setStorageUriNew("ENR");
			}
		}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
			if(UpdateProcessUtil.isNewClipOrHCPExtensionRequired(metadata, retentionEndNew, historyByGuid)){
				analysisMetadata.setStorageUriNew("R");
			}else{
				analysisMetadata.setStorageUriNew("NR");
			}
		}
			
		analysisMetadata.setIndexFileUriPrev(Helper.getExtendedMetadata(metadata));
		Integer indexStorageType = storageMasterMap.get(Helper.getIndexStorageId(metadata));
		if(Helper.getIndexStorageId(metadata) == null || Helper.getIndexStorageId(metadata) == 0){
			analysisMetadata.setIndexFileUriNew("NR");
		}else{
			//Storage indexStorage = Storage.findById(Helper.getIndexStorageId(metadata));
			if(indexStorageType == null){
				Logger.warn("No storage type found for this index file storage id.");
				throw new Exception("No storage type found for this index file storage id");
			}
			if(indexStorageType != null && indexStorageType == Constant.STORAGE_TYPE_HCP_ID){
				if(UpdateProcessUtil.isNewClipOrHCPExtensionRequiredForIndexFile(metadata, analysisMetadata.getStorageUriNew())){
					analysisMetadata.setIndexFileUriNew("ER");
				}else{
					analysisMetadata.setIndexFileUriNew("ENR");
				}
			}else if(indexStorageType != null && indexStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				if(UpdateProcessUtil.isNewClipOrHCPExtensionRequiredForIndexFile(metadata, analysisMetadata.getStorageUriNew())){
					analysisMetadata.setIndexFileUriNew("R");
				}else{
					analysisMetadata.setIndexFileUriNew("NR");
				}
			}
		}
		
		
		analysisMetadata.setRetentionTypePrev(getPrevRetentionType(metadata));
		analysisMetadata.setRetentionTypeNew(getNewRetentionType(metadata));
		analysisMetadata.setDbUpdateFlag("N");
					
		if(retentionEndNew > retentionEndPrev){
			analysisMetadata.setChangeType("extend");
		}else if(retentionEndNew < retentionEndPrev){
			analysisMetadata.setChangeType("shorten");	
		}else{
			analysisMetadata.setChangeType("no change");	
		}
		analysisMetadata.setEbrComment("");
		
		if(retentionTypeChangedFromEBRToFixed(metadata) && Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.UNTRIGGERED);
		}else if(retentionTypeChangedFromEBRToFixed(metadata) && Constant.ENABLED.equalsIgnoreCase(metadata.getEventBased())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.DISABLED);
		}
		
		if(retentionTypeChangedFromPermanent(metadata)){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.PERMOFF);
		}
		if(retentionTypeChangedFromEBRToPerm(metadata)  && Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.UNTRIGGERED);
		}else if(retentionTypeChangedFromEBRToPerm(metadata)  && Constant.ENABLED.equalsIgnoreCase(metadata.getEventBased())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.DISABLED);
		}
		
		if(retentionTypeEBRRetained(metadata)  && Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased()) && "shorten".equalsIgnoreCase(analysisMetadata.getChangeType())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.EBR_TO_EBR_SHO);
		}else if(retentionTypeEBRRetained(metadata)  && Constant.TRIGGERED.equalsIgnoreCase(metadata.getEventBased()) && "extend".equalsIgnoreCase(analysisMetadata.getChangeType())){
			if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_HCP_ID){
				analysisMetadata.setStorageUriNew("ENR");
				analysisMetadata.setIndexFileUriNew("ENR");
			}else if(archiveStorageType != null && archiveStorageType == Constant.STORAGE_TYPE_CENTERA_ID){
				analysisMetadata.setStorageUriNew("NR");
				analysisMetadata.setIndexFileUriNew("NR");
			}
			analysisMetadata.setEbrComment(Constant.EBR_TO_EBR_EXT);
		}
		return analysisMetadata;
	}
	
	private static List<Metadata> getImpactedDataForParameters(Integer cycleId, String ait, String projectId, String recordCode)  throws Exception{
		if(ait != null && projectId != null && recordCode != null){
			return Metadata.listByAitAndProjectidAndRecordCode(cycleId, ait, projectId, recordCode);
		}else if(ait != null && projectId != null && recordCode == null){
			return Metadata.listByAitAndProjectid(cycleId, ait, projectId);
		}else if(ait != null && projectId == null && recordCode != null){
			return Metadata.listByAitAndRecordCode(cycleId, ait, recordCode);
		}else if(ait == null && projectId != null && recordCode != null){
			return Metadata.listByProjectidAndRecordCode(cycleId, projectId, recordCode);
		}else if(ait != null && projectId == null && recordCode == null){
			return Metadata.listByAit(cycleId, ait);
		}else if(ait == null && projectId != null && recordCode == null){
			return Metadata.listByProjectId(cycleId, projectId);
		}else if(ait == null && projectId == null && recordCode != null){
			return Metadata.listByRecordCode(cycleId, recordCode);
		}
		
		return null;
	}
	
	private static Long getNewRetentionEnd(Metadata metadata){
		//calculate new retention end date based on the retention start and record code
		Long newRetention = 0L;
		RetentionUpdates update= retentionUpdatePerRecordCodeAndCountryCode.get(metadata.getRecordCode()+","+metadata.getCountry());
		if("Permanent".equalsIgnoreCase(update.getEventType())){
			newRetention = null;
			return newRetention;
		}
		Long ret = update.getRetention();
		TimeUnit timeUnit = Helper.getTimeUnit(update.getTimeUnit());
		newRetention = metadata.getRetentionStart() + Helper.getRecordCodeSeconds(ret, timeUnit) * 1000L;
		return newRetention;
	}
	
	private static List<RetentionUpdates> getAllRetentionUpdatesForCycle(Integer cycleId) {
		return RetentionUpdates.listByCycleId(cycleId);
	}
	
	private static void prepareRetentionUpdatesMap(List<RetentionUpdates> retentionUpdates) {
		retentionUpdatePerRecordCodeAndCountryCode = new HashMap<String,RetentionUpdates>();
		for(RetentionUpdates updates : retentionUpdates){
			retentionUpdatePerRecordCodeAndCountryCode.put(updates.getRecordCode() + ","+updates.getCountryCode(), updates);
		}
	}
	
	private static boolean retentionTypeChanged(Metadata metadata) {
		if(!getPrevRetentionType(metadata).equalsIgnoreCase(getNewRetentionType(metadata))){
			return true;
		}
		return false;
	}

	private static boolean retentionTypeChangedFromEBRToFixed(Metadata metadata) {
		if("Event + Fixed Time".equalsIgnoreCase(getPrevRetentionType(metadata)) && "Fixed Time".equalsIgnoreCase(getNewRetentionType(metadata))){
			return true;
		}
		return false;
	}


	private static boolean retentionTypeChangedFromPermanent(Metadata metadata) {
		if("Permanent".equalsIgnoreCase(getPrevRetentionType(metadata)) && ("Event + Fixed Time".equalsIgnoreCase(getNewRetentionType(metadata)) || "Fixed Time".equalsIgnoreCase(getNewRetentionType(metadata)))){
			return true;
		}
		return false;
	}

	private static boolean retentionTypePermanentRetained(Metadata metadata) {
		if("Permanent".equalsIgnoreCase(getPrevRetentionType(metadata)) && "Permanent".equalsIgnoreCase(getNewRetentionType(metadata))){
			return true;
		}
		return false;
	}

	private static boolean retentionTypeChangedFromEBRToPerm(Metadata metadata) {
		if("Event + Fixed Time".equalsIgnoreCase(getPrevRetentionType(metadata)) && "Permanent".equalsIgnoreCase(getNewRetentionType(metadata))){
			return true;
		}
		return false;
	}
	
	private static boolean retentionTypeEBRRetained(Metadata metadata) {
		if("Event + Fixed Time".equalsIgnoreCase(getPrevRetentionType(metadata)) && "Event + Fixed Time".equalsIgnoreCase(getNewRetentionType(metadata))){
			return true;
		}
		return false;
	}
	
	private static String getPrevRetentionType(Metadata m){
		//get it from RETENTION_UPDATES table
		RetentionUpdates ru = retentionUpdatePerRecordCodeAndCountryCode.get(m.getRecordCode()+","+m.getCountry());
		if(ru != null){
			return ru.getUsOfficialEventTypeHistory();
		}
		return null;
	}
	
	private static String getNewRetentionType(Metadata m){
		RetentionUpdates ru = retentionUpdatePerRecordCodeAndCountryCode.get(m.getRecordCode()+","+m.getCountry());
		if(ru != null){
			return ru.getEventType();
		}
		return null;
	}
	
	private static Map<String, List<MetadataHistory>> getMetdataHistoryForImpactedData(Integer cycleId, String ait, String projectId, String recordCode, boolean runForAll) throws Exception {
		Logger.info("getMetdataHistoryForImpactedData started");
		Map<String, List<MetadataHistory>> historyByGuid = new HashMap<String, List<MetadataHistory>>();
		List<MetadataHistory> hist = null;
		if(runForAll){
			hist = MetadataHistory.getMetdataHistoryForImpactedData(cycleId);
		}else{
			hist = getMetdataHistoryForImpactedDataWithParameter(cycleId, ait,projectId,recordCode);
		}
		
		Logger.info("getMetdataHistoryForImpactedData got all history");
		if(!hist.isEmpty()){
			String prevGuid = hist.get(0).getGuid();
			List<MetadataHistory> m = new ArrayList<MetadataHistory>();
			
			for(MetadataHistory h : hist){
				if(h.getGuid().equalsIgnoreCase(prevGuid)){
					m.add(h);
					prevGuid = h.getGuid();
				}else{
					historyByGuid.put(prevGuid, m);
					m = new ArrayList<MetadataHistory>();
					m.add(h);
					prevGuid = h.getGuid();
				}
				
			}
		}
		Logger.info("getMetdataHistoryForImpactedData exiting");
		return historyByGuid;
	}
	
	private static List<MetadataHistory> getMetdataHistoryForImpactedDataWithParameter(Integer cycleId,String ait, String projectId, String recordCode)  throws Exception{
		if(ait != null && projectId != null && recordCode != null){
			return MetadataHistory.listByAitAndProjectidAndRecordCode(cycleId, ait, projectId, recordCode);
		}else if(ait != null && projectId != null && recordCode == null){
			return MetadataHistory.listByAitAndProjectid(cycleId, ait, projectId);
		}else if(ait != null && projectId == null && recordCode != null){
			return MetadataHistory.listByAitAndRecordCode(cycleId, ait, recordCode);
		}else if(ait == null && projectId != null && recordCode != null){
			return MetadataHistory.listByProjectidAndRecordCode(cycleId, projectId, recordCode);
		}else if(ait != null && projectId == null && recordCode == null){
			return MetadataHistory.listByAit(cycleId, ait);
		}else if(ait == null && projectId != null && recordCode == null){
			return MetadataHistory.listByProjectId(cycleId, projectId);
		}else if(ait == null && projectId == null && recordCode != null){
			return MetadataHistory.listByRecordCode(cycleId, recordCode);
		}
		
		return null;
	}
	
	private static boolean runForAll(String ait, String projectId, String recordCode){
		if("all".equalsIgnoreCase(ait) && "all".equalsIgnoreCase(projectId) && "all".equalsIgnoreCase(recordCode)){
			return true;
		}
		return false;
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
