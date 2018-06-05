package jobs.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utilities.Constant;
import models.storageapp.AppConfigProperty;
import models.storageapp.Helper;
import models.storageapp.Metadata;
import models.storageapp.MetadataAnalysis;
import models.storageapp.MetadataHistory;
import models.storageapp.RCACycle;
import models.storageapp.RCACycleHistory;
import models.storageapp.RetentionCycle;
import models.storageapp.RetentionCycleHistory;
import models.storageapp.Storage;
import play.Logger;

public class UpdateProcessUtil {

	
	public static boolean isNewClipOrHCPExtensionRequiredForIndexFile(Metadata metadata, String storageUriNew) {
		if(null != Helper.getExtendedMetadata(metadata) && null != metadata.getMetadataUrl() && "R".equalsIgnoreCase(storageUriNew)){
			return true;
		}else if(null != Helper.getExtendedMetadata(metadata) && null != metadata.getMetadataUrl() && "ER".equalsIgnoreCase(storageUriNew)){
			return true;
		}else{
			return false;
		}
		
	}
	
	public static boolean isNewClipOrHCPExtensionRequired(Metadata metadata, Long newRetentionEnd, Map<String, List<MetadataHistory>> historyByGuid){
		//get all retention end dates from history table if the new retention end date is greater then all then yes else no
		//List<MetadataHistory> allMetadata = MetadataHistory.listByGuid(metadata.getGuid());
		List<MetadataHistory> allMetadata = historyByGuid.get(metadata.getGuid());
		
		List<Long> retentionEndDates = new ArrayList<Long>();
		if(allMetadata != null){
			for(MetadataHistory h : allMetadata){
				if(h.getRetentionEndPrev() == null){
					retentionEndDates.add(Constant.MAX_DATE);  //max date : 12/01/9999;
				}else{
					retentionEndDates.add(h.getRetentionEndPrev());
				}
			}
		}
		Long metadataEndDate = metadata.getRetentionEnd();
		if(null == metadataEndDate){
			metadataEndDate = Constant.MAX_DATE; //253399640400000L;
		}
		retentionEndDates.add(metadataEndDate);
		Collections.sort(retentionEndDates);
		if(newRetentionEnd > retentionEndDates.get(retentionEndDates.size()-1)){
			return true;
		}else{
			return false;
		}
	}
	
	public static Map<Integer, Integer> getStorageTypeAndIdMapping() {
		List<Storage> storageMaster = Storage.findAll();
		Map<Integer, Integer> storageMasterMap = new HashMap<Integer, Integer>();
		for(Storage st : storageMaster){
			storageMasterMap.put(st.getId(), st.getStorageType());
		}
		return storageMasterMap;
	}

	public static String getProperty(String property) {
		AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(property);
   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
			Logger.warn("UpdateProcessUtil - "+property + " not found");
			return "";
		}else{
			return appConfigProperty.getValue();
		}
	}
	
	public static boolean analysisAlreadyValidated(Integer currentStatus) {
		if(currentStatus >= Constant.CYCLE_STATUS_ANALYSIS_VALIDATED){
			return true;
		}else{
			return false;
		}
		
	}
	
	public static boolean updatesFullyCompleted(Integer currentStatus) {
		if(currentStatus >= Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED){
			return true;
		}else{
			return false;
		}
	}

	public static boolean allUpdated() {
		if(MetadataAnalysis.allWithDBUpdatedNo().isEmpty()){
			return true;
		}else{
			return false;
		}
		
	}
	
	public static RCACycle getCurrentRCACycle(){
		//get current cycle from RCA_CYCLE table
		return RCACycle.getCurrentCycleDetails();
	}
	
	
	public static RetentionCycle getCurrentRetentionCycle(){
		//get current cycle from RETENTION_CYCLE table
		return RetentionCycle.getCurrentCycleDetails();
	}
	
	public static void updateRetentionCycleStatus(RetentionCycle currentRetentionCycle, int status, String executionMode, String analysisValidationMode){
		//update status in retention_cycle table
		currentRetentionCycle.setCurrentStatus(status);
		currentRetentionCycle.setAnalysisValidationMode(analysisValidationMode);
		currentRetentionCycle.setExecutionMode(executionMode);
		currentRetentionCycle.setCycleDate(new Date(System.currentTimeMillis()));
		currentRetentionCycle.save();
	}
	
	public static void insertInRetentionCycleHistory(Integer cycleId, int status, String executionMode, String analysisValidationMode){
		//insert status in retention_cycle_history table
		RetentionCycleHistory history = new RetentionCycleHistory();
		history.setId(RetentionCycleHistory.getGeneratedRetentionCycleHistoryId()); 
		history.setCycleId(cycleId);
		history.setStatus(status);
		history.setAnalysisValidationMode(analysisValidationMode);
		history.setExecutionMode(executionMode);
		history.setCycleDate(new Date(System.currentTimeMillis()));
		history.save();
	}
	
	public static void updateRCACycleStatus(RCACycle currentCycle, int status,String executionMode, String analysisValidationMode){
		//update status in rca_cycle table
		currentCycle.setCurrentStatus(status);
		currentCycle.setExecutionMode(executionMode);
		currentCycle.setAnalysisValidationMode(analysisValidationMode);
		currentCycle.setCycleDate(new Date(System.currentTimeMillis()));
		currentCycle.save();
	}
	
	public static void insertInRCACycleHistory(Integer cycleId, int status, String executionMode, String analysisValidationMode){
		//insert status in rca_cycle_history table
		RCACycleHistory history = new RCACycleHistory();
		history.setId(RCACycleHistory.getGeneratedRCACycleHistoryId()); 
		history.setCycleId(cycleId);
		history.setStatus(status);
		history.setExecutionMode(executionMode);
		history.setAnalysisValidationMode(analysisValidationMode);
		history.setCycleDate(new Date(System.currentTimeMillis()));
		history.save();
	}
}
