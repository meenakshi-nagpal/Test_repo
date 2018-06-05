package jobs.aims;


import java.util.List;

import jobs.util.JobsUtil;
import jobs.util.UpdateProcessUtil;
import models.storageapp.RetentionCycle;
import models.storageapp.RCAUpdates;
import models.storageapp.AppConfigProperty;
import models.storageapp.RCACycle;
import models.storageapp.RCACycleHistory;

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
import valueobjects.RecordCodeVO;
import valueobjects.RecordCodeAlignmentDetailsVO;
import ws.AIMSWSClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GetRecordCodeAlignmentDetailsJob implements Job{
	private static Scheduler scheduler = null;
	private static final String GetRecordCodeAlignmentDetailsJob = "GetRecordCodeAlignmentDetailsJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("GetRecordCodeAlignmentDetailsJob"))
				return;
			Logger.info("GetRecordCodeAlignmentDetailsJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.GET_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("GetRecordCodeAlignmentDetailsJob - "+Constant.GET_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR + " not found - skiplaypping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(GetRecordCodeAlignmentDetailsJob.class).withIdentity(new JobKey("GetRecordCodeAlignmentDetailsJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(GetRecordCodeAlignmentDetailsJob, Constant.RETENTION_UPDATE_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("GetRecordCodeAlignmentDetailsJob - Failed to schedule job", e);
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
					Logger.warn("GetRecordCodeAlignmentDetailsJob - Trigger/Key is null");
					return "GetRecordCodeAlignmentDetailsJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("GetRecordCodeAlignmentDetailsJob - scheduler is null");
				return "GetRecordCodeAlignmentDetailsJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("GetRecordCodeAlignmentDetailsJob - Exception in unscheduling the job "+e);
			return "GetRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("GetRecordCodeAlignmentDetailsJob - scheduler is already running");
				return "GetRecordCodeAlignmentDetailsJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("GetRecordCodeAlignmentDetailsJob - Exception in rescheduling the job "+e);
			return "GetRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	public static String runOnDemand(){
		try
		{
		/*	if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("GetRecordCodeAlignmentDetailsJob - scheduler is already running");
				return "GetRecordCodeAlignmentDetailsJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
			*/
				
				SendPendingAlertConfirmationToAims();
				String response = GetRecordCodeAlignmentUpdateFromAims(Constant.ONDEMAND_RUN);
				
				if(response==null || response.equalsIgnoreCase(Constant.SUCCESS)){
					return Constant.SUCCESS;	
				}else{
					return Constant.SUCCESS_WITH_WARNING+" ,"+response;
				}
			//}
		}catch(Exception e)
		{
			Logger.error("GetRecordCodeAlignmentDetailsJob - Exception in rescheduling the job "+e);
			return "GetRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("GetRecordCodeAlignmentDetailsJob"))
			{
				Logger.warn("GetRecordCodeAlignmentDetailsJob - Job is not allowed to run on this host ");
				return;
			}			
			
			SendPendingAlertConfirmationToAims();
			GetRecordCodeAlignmentUpdateFromAims(Constant.SCHEDULED_RUN);
	
			Logger.info("GetRecordCodeAlignmentDetailsJob - Executing the job done");
			
			return;
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
	}
	
	public static String GetRecordCodeAlignmentUpdateFromAims(String executionMode){
		
		try{
			
			Logger.info("GetRecordCodeAlignmentDetailsJob - Started ");
			Logger.info("GetRecordCodeAlignmentDetailsJob - Checking Retention Update Cycle Status ");
			
			RetentionCycle retentionCycle =getRetentionCurrentCycleDetails();
			if(retentionCycle==null || (retentionCycle.getCurrentStatus()!=null && retentionCycle.getCurrentStatus()==Constant.CYCLE_STATUS_CYCLE_COMPLETED)  ){
				
				Logger.info("GetRecordCodeAlignmentDetailsJob - Retention Update Cycle Status is not in progress");
			}else{
				Logger.info("GetRecordCodeAlignmentDetailsJob - Retention Update Cycle is in progress, cannot proceed ");
				return "GetRecordCodeAlignmentDetailsJob - Retention Update Cycle is in progress, cannot proceed ";
			}
			
			
			RCACycle rcaCycle =getRCACurrentCycleDetails();
			String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RCA_RUN_MODE);			
			
			// check if status is cycle_completed if yes then proceed
			if(rcaCycle==null || rcaCycle.getCurrentStatus()==null || rcaCycle.getCurrentStatus()==Constant.CYCLE_STATUS_CYCLE_COMPLETED  ){
				
			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_RECORD_CODE_ALIGNMENT_DETAILS_LIST_URL);
			String sourceSystemId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
			

			if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
				Logger.error("GetRecordCodeAlignmentDetailsJob AIMS Get Record Code Details list Url not found in App Config");
				Logger.debug("GetRecordCodeAlignmentDetailsJob Stopping the iteration ");
				return "GetRecordCodeAlignmentDetailsJob AIMS Get Record Code Details list Url not found in App Config";
			}
			
			
			String getRecordCodeDetailsUrl = root + (extention.replace(Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
			Logger.info("GetRecordCodeAlignmentDetailsJob AIMS Get Record Code Details list Url "+getRecordCodeDetailsUrl);
			
			
			Logger.info("GetRecordCodeAlignmentDetailsJob - Calling Aims to Get Record Code Alignment Details List");
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode aimsJsonNode = AIMSWSClient.jerseyGetAIMSJsonResponse(getRecordCodeDetailsUrl);
			
			
			if(aimsJsonNode == null || aimsJsonNode.size()==0) {
				Logger.info("GetRecordCodeAlignmentDetailsJob - Received null reponse from service.");
				return "GetRecordCodeAlignmentDetailsJob - Received null reponse from AIMS service for Record Code Updates.";
			}
			
			Logger.info("GetRecordCodeAlignmentDetailsJob - Received "+aimsJsonNode.size()+" Records from Aims service");
		  	List<RecordCodeAlignmentDetailsVO> voList = objectMapper.convertValue(aimsJsonNode, 
							new TypeReference<List<RecordCodeAlignmentDetailsVO>>() {});
			
	    	
			boolean startCycle=false;
			boolean status=false;
			RCACycle rcaCurrentCycle=null; 
			if(rcaCycle!=null ){
				if(rcaCycle.getCurrentStatus()==null)
				{   Logger.info("GetRecordCodeAlignmentDetailsJob - cycle already started.");
					startCycle=true;
					rcaCurrentCycle=rcaCycle;
				}
				
			}
			
			Logger.info("GetRecordCodeAlignmentDetailsJob - Inserting records");
			for(RecordCodeAlignmentDetailsVO recordCodeAlignmentDetailsVO : voList) {
	    		Logger.debug("GetRecordCodeAlignmentDetailsJob - recordCodeAlignmentDetailsVO "+recordCodeAlignmentDetailsVO);
					    		
	    		    		
	    		if(RCAUpdates.findByAimsId(recordCodeAlignmentDetailsVO.getId())!=null){
	    			Logger.info("GetRecordCodeAlignmentDetailsJob - Found Aims id: "+recordCodeAlignmentDetailsVO.getId()+" in Record Code Alignment Update table, so Skipping ");
	    			//check if record change already exist by aims id the skip
	    			continue;
	    		}
	    		
	    		if (recordCodeAlignmentDetailsVO.getRetentionStatus()!=null && recordCodeAlignmentDetailsVO.getRetentionStatus().equals("DATACLEANSING")){
	    			Logger.info("GetRecordCodeAlignmentDetailsJob - Retention Status is DATACLEANSING for AimsID:"+recordCodeAlignmentDetailsVO.getId()+" , so skipping");
	    			continue;
	    		}
	    		
	    		Logger.debug("startCycle - cycle status: "+startCycle);
	    		if(startCycle==false){
	    			Logger.info("GetRecordCodeAlignmentDetailsJob - Starting new Record Code Alignemnt Cycle");
	    			//starting new record Code Alignment cycle
	    			rcaCurrentCycle=startNewRCACycle(executionMode,analysisValidationMode);
	    			Logger.info("GetRecordCodeAlignmentDetailsJob - Started new Record Code Alignemnt Cycle, cycle id :"+rcaCurrentCycle.getCycleId());
	    			//insert in record Code Alignment Cycle History
	    			insertRCACycleHistory(rcaCurrentCycle,executionMode,analysisValidationMode);
	    			startCycle=true;
	    			
	    		}
	    		    		
	    		RCAUpdates rcaUpdates = new RCAUpdates();
	    		rcaUpdates.setCycleId(rcaCurrentCycle.getCycleId());
	    		rcaUpdates.setId(RCAUpdates.getGeneratedID());
	    		rcaUpdates.setAimsId(recordCodeAlignmentDetailsVO.getId());
	    		rcaUpdates.setRetention(recordCodeAlignmentDetailsVO.getUsOfficialRetention());
	    		rcaUpdates.setRetentionStatus(recordCodeAlignmentDetailsVO.getRetentionStatus());
	    		rcaUpdates.setTimeUnit(recordCodeAlignmentDetailsVO.getDispositionTimeUnit());
	    		rcaUpdates.setRecordCodePrev(recordCodeAlignmentDetailsVO.getRecordCodeHistory());
	    		rcaUpdates.setRecordCodeNew(recordCodeAlignmentDetailsVO.getRecordCode());
	    		rcaUpdates.setCountryCodeNew(((recordCodeAlignmentDetailsVO.getCountryCode()==null||recordCodeAlignmentDetailsVO.getCountryCode().trim().isEmpty()) ?"US":recordCodeAlignmentDetailsVO.getCountryCode()));
	    		rcaUpdates.setCountryCodePrev(((recordCodeAlignmentDetailsVO.getCountryCodeHistory()==null||recordCodeAlignmentDetailsVO.getCountryCodeHistory().trim().isEmpty()) ?"US":recordCodeAlignmentDetailsVO.getCountryCodeHistory()));
	    		rcaUpdates.setEventTypePrev(recordCodeAlignmentDetailsVO.getUsOfficialEventTypeHistory());
	    		rcaUpdates.setEventTypeNew(recordCodeAlignmentDetailsVO.getUsOfficialEventType());
	    		rcaUpdates.setAit(recordCodeAlignmentDetailsVO.getAit());
	    		rcaUpdates.setProjectId(recordCodeAlignmentDetailsVO.getProjectId());
	    		rcaUpdates.save();
				
				// send confirmation to aims , if status 200 update table with new status
				
				status=SendAlertConfirmationToAims(rcaUpdates);
				if(status==true){
					rcaUpdates.setIsAimsAlertNotified(Constant.trueValue);
					rcaUpdates.update();
				}
				
				
				Logger.debug("GetRecordCodeAlignmentDetailsJob - rcaUpdates "+ rcaUpdates);
			}
			
			Logger.info("GetRecordCodeAlignmentDetailsJob - Done Inserting records");
				if(rcaCurrentCycle!=null){
				updateRCACycle(rcaCurrentCycle,Constant.CYCLE_STATUS_CYCLE_STARTED,executionMode,analysisValidationMode);
				insertRCACycleHistory(rcaCurrentCycle,executionMode,analysisValidationMode);
				//return "GetRecordCodeAlignmentDetailsJob - New Record Code Alignment Details Job Cycle started.";
				return Constant.SUCCESS;
					}
				
			}
			else{
				
				Logger.info("GetRecordCodeAlignmentDetailsJob - Stopping the job as current Record Code Alignment cycle is in progress, Cycle ID : "+rcaCycle.getCycleId() +" Status : "+rcaCycle.getCurrentStatus());
				return "GetRecordCodeAlignmentDetailsJob - Stopping the job as current Record Code Alignment cycle is in progress" ;
				
			} 
			
		}
		catch (Exception e) {
			Logger.error("GetRecordCodeAlignmentDetailsJob - exception occurred while processing", e);
			e.printStackTrace();
			return "GetRecordCodeAlignmentDetailsJob - exception occurred while processing";
		} 
		
	   //return "GetRecordCodeAlignmentDetailsJob - Stopping Nothing to Process";
	   return Constant.SUCCESS;
	}
	
	public static RCACycle getRCACurrentCycleDetails(){
		
		RCACycle rcaCycle = new RCACycle();
		rcaCycle=RCACycle.getCurrentCycleDetails();
		return  rcaCycle;
		
	}
	
    public static RetentionCycle getRetentionCurrentCycleDetails(){
		
    	RetentionCycle retentionCycle = new RetentionCycle();
    	retentionCycle=RetentionCycle.getCurrentCycleDetails();
		return  retentionCycle;
		
	}
	
	public static RCACycle startNewRCACycle(String executionMode, String analysisValidationMode){
		RCACycle rcaCycle = new RCACycle();
		rcaCycle.setCycleId(RCACycle.getGeneratedCycleID());
		rcaCycle.setCurrentStatus(null);
		rcaCycle.setExecutionMode(executionMode);
		rcaCycle.setAnalysisValidationMode(analysisValidationMode);
		rcaCycle.setCycleDate(DateUtil.getDate());
		rcaCycle.save();
		return rcaCycle;
		
	}
	
	public static RCACycle updateRCACycle(RCACycle rcaCycle,int status, String executionMode, String analysisValidationMode){
		rcaCycle.setCurrentStatus(status);
		rcaCycle.setExecutionMode(executionMode);
		rcaCycle.setAnalysisValidationMode(analysisValidationMode);
		rcaCycle.setCycleDate(DateUtil.getDate());
		rcaCycle.update();
		return rcaCycle;
		
	}
	
	public static void insertRCACycleHistory(RCACycle rcaCycle, String executionMode, String analysisValidationMode){
		RCACycleHistory rcaCycleHistory = new RCACycleHistory();
		rcaCycleHistory.setId(RCACycleHistory.getGeneratedRCACycleHistoryId());
		rcaCycleHistory.setCycleId(rcaCycle.getCycleId());
		rcaCycleHistory.setStatus(rcaCycle.getCurrentStatus());
		rcaCycleHistory.setCycleDate(DateUtil.getDate());
		rcaCycleHistory.setExecutionMode(executionMode);
		rcaCycleHistory.setAnalysisValidationMode(analysisValidationMode);
		rcaCycleHistory.save();
		return;
		
	}
	
	
	public static boolean SendAlertConfirmationToAims(RCAUpdates rcaUpdate){
			
		try{
		String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_RECORD_CODE_ALIGNMENT_NOTIFIED_URL);
		String sourceSystemId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
		

		if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
			Logger.error("GetRecordCodeAlignmentDetailsJob AIMS Record Code Alignment Alert Notification Url not found in App Config");
			Logger.debug("GetRecordCodeAlignmentDetailsJob Stopping the iteration ");
			return false;
		}
		
		
		String rcaAlertUrl = root + (extention.replace(Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
		rcaAlertUrl = (rcaAlertUrl.replace(Constant.AIMS_URL_AIMS_ID_STRING,Integer.toString(rcaUpdate.getAimsId())));
		Logger.debug("GetRecordCodeAlignmentDetailsJob AIMS Alert Notification Url "+rcaAlertUrl);
		
		
		ObjectMapper objectMapper = new ObjectMapper(); 
		RecordCodeVO recordCodeVO = new RecordCodeVO();
		JsonNode jsonNode = objectMapper.convertValue(recordCodeVO, JsonNode.class);
		JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(rcaAlertUrl, jsonNode);
		
		if(responseJsonNode!=null){
			RecordCodeVO recordCodeVOUpdate = objectMapper.convertValue(responseJsonNode, new TypeReference<RecordCodeVO>() {});
			Logger.debug("GetRecordCodeAlignmentDetailsJob  responseJsonNode- "+recordCodeVOUpdate.getStatus()+"-");
			if(recordCodeVOUpdate.getStatus().equalsIgnoreCase("SUCCESS")){
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
			Logger.error("GetRecordCodeAlignmentDetailsJob - SendAlertConfirmationToAims() exception occurred while processing", e);
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static boolean SendPendingAlertConfirmationToAims(){
		
		try{
			boolean status=false;
			Logger.info("GetRecordCodeAlignmentDetailsJob - Start Pending Alert Confirmation");
			List<RCAUpdates> rcaAlertList=RCAUpdates.pendingAimsAlert();
			Logger.info("GetRecordCodeAlignmentDetailsJob - retAlertList size: "+rcaAlertList.size());
			if(rcaAlertList.size()>0){
				for(RCAUpdates rcaUpdateAlert: rcaAlertList){
					Logger.info("GetRecordCodeAlignmentDetailsJob -Aims Id - "+rcaUpdateAlert.getAimsId());
					status=SendAlertConfirmationToAims(rcaUpdateAlert);
					if(status==true){
						rcaUpdateAlert.setIsAimsAlertNotified(Constant.trueValue);
						rcaUpdateAlert.update();
					}
				}
				
			}
			else{
				Logger.info("GetRecordCodeAlignmentDetailsJob - Pending Record Code Alignment Alert list is empty so nothing to send to AIMS");
			}
		
		}catch(Exception e){
			Logger.error("GetRecordCodeAlignmentDetailsJob - SendAlertConfirmationToAims() exception occurred while processing", e);
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
}
