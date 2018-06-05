package jobs.aims;

import java.util.Date;
import java.util.List;

import jobs.util.JobsUtil;
import models.storageapp.AIMSRegistration;
import models.storageapp.AppConfigProperty;
import models.storageapp.Metadata;

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
import valueobjects.ArchiveFileRegistrationVO;
import ws.AIMSWSClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UpdateRegistrationBatchJob implements Job{
	private static Scheduler scheduler = null;
	private static final String UpdateRegistrationBatchJobTrigger = "UpdateRegistrationBatchJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("UpdateRegistrationBatchJob"))
				return;
			Logger.info("UpdateRegistrationBatchJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.REGISTRATION_UPDATE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("UpdateRegistrationBatchJob - "+Constant.REGISTRATION_UPDATE_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(UpdateRegistrationBatchJob.class).withIdentity(new JobKey("UpdateRegistrationBatchJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(UpdateRegistrationBatchJobTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("UpdateRegistrationBatchJob - Failed to schedule job", e);
			e.printStackTrace();
		}
	}

	private void updateEventBasedDetails() {
		try {
			Logger.info("UpdateRegistrationBatchJob - starting the iteration  "+DateUtil.getCurrentDate());
			List<AIMSRegistration> eventBasedUpdateList = AIMSRegistration.getAIMSRegsWithEventBasedUpdatePending();
			if (eventBasedUpdateList == null || eventBasedUpdateList.size() == 0)
			{
				Logger.warn("UpdateRegistrationBatchJob - updateEventBasedDetails() - No archives found for which event based update is pending.");
				return;	
			}
			
        	String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
        	String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_REGISTRATION_UPDATE_EVENT_BASED_URL_KEY);
		    String sourceId = AppConfigProperty.getAppConfigValue("UDAS_SOURCE_SYSTEM_ID");
		    String aimsUrl = root + extention.replace("{sourceSystem}", sourceId);
		    
		    
		    if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
		    {
		    	Logger.error("UpdateRegistrationBatchJob - "+Constant.AIMS_REGISTRATION_UPDATE_EVENT_BASED_URL_KEY + " not found - skipping AIMS registration update event based Details");
				return;
		    }

	   		//Setting AIMS update Registration url 
			for (AIMSRegistration regEntry: eventBasedUpdateList)
			{
				Metadata metadata = Metadata.findById(regEntry.getId());
		        if(metadata == null)
		        {
		            Logger.error("updateIndexDetails - Metadata Id details not found "+regEntry.getId());
		            continue;
		        }
		        else{
					if(regEntry.getAimsGuid() == null || regEntry.getAimsGuid().isEmpty()) {
						Logger.warn("UpdateRegistrationBatchJob - updateEventBasedDetails() -" +
								"Bad AIMS REGISTRATION Record for :" + 
								regEntry.getAimsGuid() + " unable to update AIMS Regsitration info.");
						continue;
					}

		        	String aimsUpdateRegistrationUrl = new String(aimsUrl);
		    		aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_PROJECT_ID_STRING, metadata.getProjectId());
		    		aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_GUID_STRING, regEntry.getAimsGuid());
		    		
		    		Logger.debug("UpdateRegistrationBatchJob - updateEventBasedDetails() - aimsUpdateRegistrationUrl "+aimsUpdateRegistrationUrl);
		    		Logger.debug("UpdateRegistrationBatchJob - updateEventBasedDetails() - AIMS guid :"+regEntry.getAimsGuid()+" event based "+metadata.getEventBased());
		    		
		    		updateAIMSEventBasedDetails(metadata,aimsUpdateRegistrationUrl,regEntry);
		        }
			}
		} catch (Exception e) {
			Logger.error("UpdateRegistrationBatchJob - exception occurred while processing", e);
			e.printStackTrace();
		}finally{
			Logger.info("UpdateRegistrationBatchJob - Ending the iteration at "+DateUtil.getCurrentDate());
		}
	}

	private void updateAIMSEventBasedDetails(Metadata metadata,String aimsUpdateRegistrationUrl,AIMSRegistration regEntry)
	{
		try
		{
			ArchiveFileRegistrationVO archiveFileRegistrationVO = new ArchiveFileRegistrationVO();
			archiveFileRegistrationVO.setEventBased(metadata.getEventBased());
			archiveFileRegistrationVO.setRetentionStartDate(DateUtil.formatDateYYYY_MM_DD(
					new Date(metadata.getRetentionStart())));
			Logger.debug("UpdateRegistrationBatchJob - registrationVO "+archiveFileRegistrationVO.getRetentionStartDate());
			
			//Making a json call to update the index file size
			ObjectMapper objectMapper = new ObjectMapper(); 
			JsonNode jsonNode = objectMapper.convertValue(archiveFileRegistrationVO, JsonNode.class);
			Logger.debug("UpdateRegistrationBatchJob - updateAIMSEventBasedDetails() - Making a jason call to AIMS");
			JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(aimsUpdateRegistrationUrl, jsonNode);
			
			ArchiveFileRegistrationVO responseArchiveFileRegistrationVO = null;
			if(responseJsonNode != null) {
				responseArchiveFileRegistrationVO = objectMapper.convertValue(responseJsonNode, ArchiveFileRegistrationVO.class);
				Logger.debug("UpdateRegistrationBatchJob - updateAIMSEventBasedDetails() - responseArchiveFileRegistrationVO" +responseArchiveFileRegistrationVO.getAimsGuid()+" "+responseArchiveFileRegistrationVO.getEventBased());
				regEntry.setStatus(Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_SUCCESS);
				regEntry.save();
			}else
			{
				Logger.warn("UpdateRegistrationBatchJob - updateAIMSEventBasedDetails() -Response from AIMS is null for : "+regEntry.getAimsGuid());
				// set status to failure so that it can be picked up by the Archive registration job
				regEntry.setStatus(Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_FAILED);
				regEntry.save();
			}
			
		}catch(Exception e)
		{
			Logger.error("UpdateRegistrationBatchJob - exception occurred while processing", e);
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
					Logger.warn("UpdateRegistrationBatchJob - Trigger/Key is null");
					return "UpdateRegistrationBatchJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("UpdateRegistrationBatchJob - scheduler is null");
				return "UpdateRegistrationBatchJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("UpdateRegistrationBatchJob - Exception in unscheduling the job "+e);
			return "UpdateRegistrationBatchJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("UpdateRegistrationBatchJob - scheduler is already running");
				return "UpdateRegistrationBatchJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("UpdateRegistrationBatchJob - Exception in rescheduling the job "+e);
			return "UpdateRegistrationBatchJob "+e.getMessage();
		}
	}
	
		
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("UpdateRegistrationBatchJob"))
			{
				Logger.warn("UpdateRegistrationBatchJob - Job is not allowed to run on this host ");
				return;
			}
			updateEventBasedDetails();
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
	}
}
