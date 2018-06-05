package jobs.aims;


import java.util.List;

import jobs.util.JobsUtil;
import jobs.util.UpdateProcessUtil;
import models.storageapp.RCACycle;
import models.storageapp.RetentionCycle;
import models.storageapp.RetentionCycleHistory;
import models.storageapp.RetentionUpdates;
import models.storageapp.AppConfigProperty;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import play.Logger;
import utilities.Constant;
import utilities.DateUtil;
import valueobjects.RetentionUpdatesVO;
import valueobjects.RecordCodeVO;
import ws.AIMSWSClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GetRetentionUpdateBatchJob implements Job{
	private static Scheduler scheduler = null;
	private static final String GetRetentionUpdateBatchJob = "GetRetentionUpdateBatchJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("GetRetentionUpdateBatchJob"))
				return;
			Logger.info("GetRetentionUpdateBatchJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.GET_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("GetRetentionUpdateBatchJob - "+Constant.GET_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR + " not found - skiplaypping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(GetRetentionUpdateBatchJob.class).withIdentity(new JobKey("GetRetentionUpdateBatchJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(GetRetentionUpdateBatchJob, Constant.RETENTION_UPDATE_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("GetRetentionUpdateBatchJob - Failed to schedule job", e);
			e.printStackTrace();
		}
	}

	
	public static String stopTrigger() {		
		try
		{
			if (scheduler != null)
			{
				if (trigger != null && trigger.getKey() != null)
				{
					scheduler.unscheduleJob(trigger.getKey());
					return Constant.SUCCESS;
				}else
				{
					Logger.warn("GetRetentionUpdateBatchJob - Trigger/Key is null");
					return "GetRetentionUpdateBatchJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("GetRetentionUpdateBatchJob - scheduler is null");
				return "GetRetentionUpdateBatchJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("GetRetentionUpdateBatchJob - Exception in unscheduling the job "+e);
			return "GetRetentionUpdateBatchJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("GetRetentionUpdateBatchJob - scheduler is already running");
				return "GetRetentionUpdateBatchJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("GetRetentionUpdateBatchJob - Exception in rescheduling the job "+e);
			return "GetRetentionUpdateBatchJob "+e.getMessage();
		}
	}
	
	public static String runOnDemand(){
		try
		{
		/*	if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("GetRetentionUpdateBatchJob - scheduler is already running");
				return "GetRetentionUpdateBatchJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
			*/
				
				SendPendingAlertConfirmationToAims();
				String response = GetRetentionUpdateFromAims(Constant.ONDEMAND_RUN);
				
				if(response==null || response.equalsIgnoreCase(Constant.SUCCESS)){
					return Constant.SUCCESS;	
				}else{
					return Constant.SUCCESS_WITH_WARNING+" ,"+response;
				}
			//}
		}catch(Exception e)
		{
			Logger.error("GetRetentionUpdateBatchJob - Exception in rescheduling the job "+e);
			return "GetRetentionUpdateBatchJob "+e.getMessage();
		}
	}
	
	
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("GetRetentionUpdateBatchJob"))
			{
				Logger.warn("GetRetentionUpdateBatchJob - Job is not allowed to run on this host ");
				return;
			}
			
			SendPendingAlertConfirmationToAims();
			GetRetentionUpdateFromAims(Constant.SCHEDULED_RUN);
			Logger.info("GetRetentionUpdateBatchJob - Executing the job done");
			
			return;
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
	}
	
	public static String GetRetentionUpdateFromAims(String executionMode){
		
		try{
			
			Logger.info("GetRetentionUpdateBatchJob - Started ");
			 Logger.info("GetRetentionUpdateBatchJob - Checking Record Code Alignment Cycle Status ");
			
			 String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_RUN_MODE);
				
			 
				RCACycle rcaCycle =getRCACurrentCycleDetails();
				if(rcaCycle==null || (rcaCycle.getCurrentStatus()!=null && rcaCycle.getCurrentStatus()==Constant.CYCLE_STATUS_CYCLE_COMPLETED)  ){
					
					Logger.info("GetRetentionUpdateBatchJob - Record Code Alignment Cycle Status is not in progress");
				}else{
					
					Logger.info("GetRetentionUpdateBatchJob - Record Code Alignment Cycle Status is in progress, cannot proceed ");
					return "GetRetentionUpdateBatchJob - Record Code Alignment Cycle Status is in progress, cannot proceed ";
				}
				
			RetentionCycle retentionCycle =getCurrentCycleDetails();
						
			
			// check if status is cycle_completed if yes then proceed
			if(retentionCycle==null || retentionCycle.getCurrentStatus()==null || retentionCycle.getCurrentStatus()==Constant.CYCLE_STATUS_CYCLE_COMPLETED  ){
				
			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_RETENTION_UPDATE_LIST_URL);
			String sourceSystemId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
			

			if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
				Logger.error("GetRetentionUpdateBatchJob AIMS Get Retention Update Url not found in App Config");
				Logger.debug("GetRetentionUpdateBatchJob Stopping the iteration ");
				return "GetRetentionUpdateBatchJob AIMS Get Retention Update Url not found in App Config";
			}
			
		
			String getRetentionupdateUrl = root + (extention.replace(Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
			Logger.debug("GetRetentionUpdateBatchJob AIMS Get Retention Update Url "+getRetentionupdateUrl);
			
			
			Logger.info("GetRetentionUpdateBatchJob - Calling Aims to Get Retention Update List");
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode aimsJsonNode = AIMSWSClient.jerseyGetAIMSJsonResponse(getRetentionupdateUrl);
			
			
			if(aimsJsonNode == null || aimsJsonNode.size()==0) {
				Logger.info("GetRetentionUpdateBatchJob - Received null reponse from AIMS service for Retention Updates List.");
				return "GetRetentionUpdateBatchJob - Received null reponse from AIMS service for Retention Updates.";
			}
			
			Logger.info("GetRetentionUpdateBatchJob - Received "+aimsJsonNode.size()+" Retention Updated Records from Aims service");
		  	List<RetentionUpdatesVO> voList = objectMapper.convertValue(aimsJsonNode, 
							new TypeReference<List<RetentionUpdatesVO>>() {});
			
	    	
			boolean startCycle=false;
			boolean status=false;
			RetentionCycle retCycle=null; 
			if(retentionCycle!=null ){
				if(retentionCycle.getCurrentStatus()==null)
				{   Logger.info("GetRetentionUpdateBatchJob - cycle already started.");
					startCycle=true;
					retCycle=retentionCycle;
				}
				
			}
			
			Logger.info("GetRetentionUpdateBatchJob - Inserting Records");
			for(RetentionUpdatesVO retentionUpdatesVO : voList) {
	    		Logger.debug("GetRetentionUpdateBatchJob - retentionUpdatesVO "+retentionUpdatesVO);
					    		
	    		    		
	    		if(RetentionUpdates.findByAimsId(retentionUpdatesVO.getId())!=null){
	    			Logger.info("GetRetentionUpdateBatchJob - Found Aims id: "+retentionUpdatesVO.getId()+" in Retention Update table, so Skipping ");
	    			//check if record change already exist by aims id the skip
	    			continue;
	    		}
	    		
	    		if (retentionUpdatesVO.getRetentionStatus()!=null && retentionUpdatesVO.getRetentionStatus().equals("DATACLEANSING")){
	    			Logger.info("GetRetentionUpdateBatchJob - Retention Status is DATACLEANSING for AimsID:"+retentionUpdatesVO.getId()+" , so skipping");
	    			continue;
	    		}
	    		
	    		Logger.debug("startCycle - cycle status: "+startCycle);
	    		if(startCycle==false){
	    			Logger.info("GetRetentionUpdateBatchJob - Starting new Retention Cycle");
	    			//starting new retention cycle
	    			retCycle=startNewRetentionCycle(executionMode, analysisValidationMode);
	    			Logger.info("GetRetentionUpdateBatchJob - Started new Retention Cycle, cycle id :"+retCycle.getCycleId());
	    			//insert in Retention Cycle History
	    			insertRetentionCycleHistory(retCycle,executionMode, analysisValidationMode);
	    			startCycle=true;
	    			
	    		}
	    		
	    		    		
				RetentionUpdates retentionUpdates = new RetentionUpdates();
				retentionUpdates.setCycleId(retCycle.getCycleId());
				retentionUpdates.setId(RetentionUpdates.getGeneratedID());
				retentionUpdates.setAimsId(retentionUpdatesVO.getId());
				retentionUpdates.setRecordCode(retentionUpdatesVO.getRecordCode());
				retentionUpdates.setCountryCode(((retentionUpdatesVO.getCountryCode()==null||retentionUpdatesVO.getCountryCode().trim().isEmpty()) ?"US":retentionUpdatesVO.getCountryCode()));
				retentionUpdates.setEventType(retentionUpdatesVO.getUsOfficialEventType());
				retentionUpdates.setRetention(retentionUpdatesVO.getUsOfficialRetention());
				retentionUpdates.setRetentionStatus(retentionUpdatesVO.getRetentionStatus()); 
				retentionUpdates.setTimeUnit(retentionUpdatesVO.getDispositionTimeUnit());
				//retentionUpdates.setIsEventTypeChanged((retentionUpdatesVO.getIsEventTypeChanged()==true?'Y':'N'));
				retentionUpdates.setUsOfficialEventTypeHistory(retentionUpdatesVO.getUsOfficialEventTypeHistory());
				retentionUpdates.save();
				
				// send confirmation to aims , if status 200 update table with new status
				
				status=SendAlertConfirmationToAims(retentionUpdates);
				if(status==true){
					retentionUpdates.setIsAimsAlertNotified(Constant.trueValue);
					retentionUpdates.update();
				}
				
				
				Logger.debug("GetRetentionUpdateBatchJob - retentionUpdates "+ retentionUpdates);
			}
			Logger.info("GetRetentionUpdateBatchJob - Done Inserting Records");
				if(retCycle!=null){
				updateRetentionCycle(retCycle,Constant.CYCLE_STATUS_CYCLE_STARTED, executionMode, analysisValidationMode);
				insertRetentionCycleHistory(retCycle,executionMode, analysisValidationMode);
				//return "GetRetentionUpdateBatchJob - New Retention Schedule Update Cycle started.";
				return Constant.SUCCESS;
					}
				
			}
			else{
				
				Logger.info("GetRetentionUpdateBatchJob - Stopping the job as current retention cycle is in progress, current retention cycle status : "+retentionCycle.getCurrentStatus());
				return "GetRetentionUpdateBatchJob - Stopping the job as current retention cycle is in progress" ;
				
			} 
			
		}
		catch (Exception e) {
			Logger.error("GetRetentionUpdateBatchJob - exception occurred while processing", e);
			e.printStackTrace();
			return "GetRetentionUpdateBatchJob - exception occurred while processing";
		} 
		
	   //return "GetRetentionUpdateBatchJob - Stopping Nothing to Process";
	   return Constant.SUCCESS;
	}
	
	public static RetentionCycle getCurrentCycleDetails(){
		
		RetentionCycle retentionCycle = new RetentionCycle();
		retentionCycle=RetentionCycle.getCurrentCycleDetails();
		return  retentionCycle;
		
	}
	
	public static RCACycle getRCACurrentCycleDetails(){
			
			RCACycle rcaCycle = new RCACycle();
			rcaCycle=RCACycle.getCurrentCycleDetails();
			return  rcaCycle;
			
	}
	
	public static RetentionCycle startNewRetentionCycle(String executionMode, String analysisValidationMode){
		RetentionCycle retentionCycle = new RetentionCycle();
		retentionCycle.setCycleId(RetentionCycle.getGeneratedCycleID());
		retentionCycle.setCurrentStatus(null);
		retentionCycle.setAnalysisValidationMode(analysisValidationMode);
		retentionCycle.setExecutionMode(executionMode);
		retentionCycle.setCycleDate(DateUtil.getDate());
		retentionCycle.save();
		return retentionCycle;
		
	}
	
	public static RetentionCycle updateRetentionCycle(RetentionCycle retentionCycle,int status, String executionMode, String analysisValidationMode){
		retentionCycle.setCurrentStatus(status);
	    retentionCycle.setCycleDate(DateUtil.getDate());
	    retentionCycle.setAnalysisValidationMode(analysisValidationMode);
	    retentionCycle.setExecutionMode(executionMode);
		retentionCycle.update();
		return retentionCycle;
		
	}
	
	public static void insertRetentionCycleHistory(RetentionCycle retentionCycle, String executionMode, String analysisValidationMode){
		RetentionCycleHistory retentionCycleHistory = new RetentionCycleHistory();
		retentionCycleHistory.setId(RetentionCycleHistory.getGeneratedRetentionCycleHistoryId());
		retentionCycleHistory.setCycleId(retentionCycle.getCycleId());
		retentionCycleHistory.setStatus(retentionCycle.getCurrentStatus());
		retentionCycleHistory.setAnalysisValidationMode(analysisValidationMode);
		retentionCycleHistory.setExecutionMode(executionMode);
		retentionCycleHistory.setCycleDate(DateUtil.getDate());
		retentionCycleHistory.save();
		return;
		
	}
	
		
	public static boolean SendAlertConfirmationToAims(RetentionUpdates retentionUpdate){
			
		try{
		String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_RETENTION_ALERT_NOTIFIED_URL);
		String sourceSystemId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
		

		if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
			Logger.error("GetRetentionUpdateBatchJob AIMS Alert Notification Url not found in App Config");
			Logger.debug("GetRetentionUpdateBatchJob Stopping the iteration ");
			return false;
		}
		
		
		String retentionAlertUrl = root + (extention.replace(Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
		retentionAlertUrl = (retentionAlertUrl.replace(Constant.AIMS_URL_AIMS_ID_STRING,Integer.toString(retentionUpdate.getAimsId())));
		Logger.debug("GetRetentionUpdateBatchJob AIMS Alert Notification Url "+retentionAlertUrl);
		
		
		ObjectMapper objectMapper = new ObjectMapper(); 
		RecordCodeVO recordCodeVO = new RecordCodeVO();
		JsonNode jsonNode = objectMapper.convertValue(recordCodeVO, JsonNode.class);
		JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(retentionAlertUrl, jsonNode);
		
		if(responseJsonNode!=null){
			RecordCodeVO recordVOUpdate = objectMapper.convertValue(responseJsonNode, new TypeReference<RecordCodeVO>() {});
			Logger.debug("GetRetentionUpdateBatchJob  responseJsonNode- "+recordVOUpdate.getStatus()+"-");
			if(recordVOUpdate.getStatus().equalsIgnoreCase("SUCCESS")){
					return true;
			}
			else{
					return false;
				}
		}
		else{
			
			return false;
		}
		
		}catch(Exception e){
			Logger.error("GetRetentionUpdateBatchJob - SendAlertConfirmationToAims() exception occurred while processing", e);
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static boolean SendPendingAlertConfirmationToAims(){
		
		try{
			boolean status=false;
			Logger.info("GetRetentionUpdateBatchJob - Started Pending Alert Confirmation");
			List<RetentionUpdates> retAlertList=RetentionUpdates.pendingAimsAlert();
			Logger.info("GetRetentionUpdateBatchJob - retAlertList: "+retAlertList.size());
			if(retAlertList.size()>0){
				for(RetentionUpdates retUpdateAlert: retAlertList){
					Logger.info("GetRetentionUpdateBatchJob -Aims Id - "+retUpdateAlert.getAimsId());
					status=SendAlertConfirmationToAims(retUpdateAlert);
					if(status==true){
					retUpdateAlert.setIsAimsAlertNotified(Constant.trueValue);
					retUpdateAlert.update();
					}
				}
				
			}
			else{
				Logger.info("GetRetentionUpdateBatchJob - Pending Retention Updates Alert list is empty so nothing to send to AIMS");
			}
		
		}catch(Exception e){
			Logger.error("GetRetentionUpdateBatchJob - SendAlertConfirmationToAims() exception occurred while processing", e);
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
}
