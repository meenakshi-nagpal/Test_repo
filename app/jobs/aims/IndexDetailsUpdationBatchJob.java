package jobs.aims;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jobs.util.JobsUtil;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.StdSchedulerFactory;

import models.storageapp.AIMSRegistration;
import models.storageapp.AppConfigProperty;
import models.storageapp.Metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import play.Logger;
import utilities.Constant;
import utilities.DateUtil;
import valueobjects.ArchiveFileRegistrationVO;
import ws.AIMSWSClient;

@DisallowConcurrentExecution
public class IndexDetailsUpdationBatchJob implements Job{
	
	private static Scheduler scheduler = null;
	private static final String IndexDetailsUpdationBatchJobTrigger = "IndexDetailsUpdationBatchJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
			Logger.info("IndexDetailsUpdationBatchJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.INDEX_UPDATE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("IndexDetailsUpdationBatchJob - "+Constant.INDEX_UPDATE_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(IndexDetailsUpdationBatchJob.class).withIdentity(new JobKey("IndexDetailsUpdationBatchJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(IndexDetailsUpdationBatchJobTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("IndexDetailsUpdationBatchJob - Failed to schedule job", e);
			e.printStackTrace();
		}
	}

	private void updateIndexDetails() {
		try {
			Logger.info("IndexDetailsUpdationBatchJob - starting the iteration  "+DateUtil.getCurrentDate());
			List<AIMSRegistration> indexFailedList = AIMSRegistration.getAIMSRegsWithIndexUpdatesFailed();
			if (indexFailedList == null || indexFailedList.size() == 0)
			{
				Logger.warn("IndexDetailsUpdationBatchJob - updateIndexDetails() - No Index failures Found for Updation.");
				return;	
			}
			
        	String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		    String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_REGISTRATION_UPDATE_INDEX_URL_KEY);
		    String sourceId = AppConfigProperty.getAppConfigValue("UDAS_SOURCE_SYSTEM_ID");
		    String aimsUrl = root + extention.replace("{sourceSystem}", sourceId);
		    
		    if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
		    {
		    	Logger.error("IndexDetailsUpdationBatchJob - "+Constant.AIMS_REGISTRATION_UPDATE_INDEX_URL_KEY + " not found - skipping AIMS registration update Index Details");
				return;
		    }

	   		//Setting AIMS update Registration url 
			for (AIMSRegistration regEntry: indexFailedList)
			{
				Metadata metadata = Metadata.findById(regEntry.getId());
		        if(metadata == null)
		        {
		            Logger.error("updateIndexDetails - Metadata Id details not found "+regEntry.getId());
		            continue;
		        }
		        else{
					if(regEntry.getAimsGuid() == null || regEntry.getAimsGuid().isEmpty()) {
						Logger.warn("IndexDetailsUpdationBatchJob - updateIndexDetails() -" +
								"Bad AIMS REGISTRATION Record for :" + 
								regEntry.getAimsGuid() + " unable to update AIMS Regsitration info.");
						continue;
					}

		        	String aimsUpdateRegistrationUrl = new String(aimsUrl);
		    		aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_PROJECT_ID_STRING, metadata.getProjectId());
		    		aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_GUID_STRING, regEntry.getAimsGuid());
		    		
		    		Logger.debug("IndexDetailsUpdationBatchJob - updateIndexDetails() - aimsUpdateRegistrationUrl "+aimsUpdateRegistrationUrl);
		    		Logger.debug("IndexDetailsUpdationBatchJob - updateIndexDetails() - AIMS guid :"+regEntry.getAimsGuid()+" index size "+metadata.getMetadataSize());
		    		
		    		//aimsUpdateRegistrationUrl="http://localhost:8080/AIMS/services/cars/sourceSystem/1/archiveProject/{projectId}/registrations/{aimsGuid}";
		    		//aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_PROJECT_ID_STRING, metadata.getProjectId());
		    		//aimsUpdateRegistrationUrl = aimsUpdateRegistrationUrl.replace(Constant.AIMS_URL_GUID_STRING, regEntry.getAimsGuid());
		    		
		        	updateAIMSIndexFileDetails(metadata,aimsUpdateRegistrationUrl,regEntry);
		        }
			}
		} catch (Exception e) {
			Logger.error("IndexDetailsUpdationBatchJob - exception occurred while processing", e);
			e.printStackTrace();
		}finally{
			Logger.info("IndexDetailsUpdationBatchJob - Ending the iteration at "+DateUtil.getCurrentDate());
		}
	}

	private void updateAIMSIndexFileDetails(Metadata metadata,String aimsUpdateRegistrationUrl,AIMSRegistration regEntry)
	{
		try
		{
			ArchiveFileRegistrationVO archiveFileRegistrationVO = new ArchiveFileRegistrationVO();
			archiveFileRegistrationVO.setIndexFileSize(metadata.getMetadataSize());
			
			//Making a json call to update the index file size
			ObjectMapper objectMapper = new ObjectMapper(); 
			JsonNode jsonNode = objectMapper.convertValue(archiveFileRegistrationVO, JsonNode.class);
			Logger.debug("IndexDetailsUpdationBatchJob - updateAIMSIndexFileDetails() - Making a jason call to AIMS");
			JsonNode responseJsonNode = AIMSWSClient.jerseyPutToAIMSAndGetJsonResponse(aimsUpdateRegistrationUrl, jsonNode);
			
			ArchiveFileRegistrationVO responseArchiveFileRegistrationVO = null;
			if(responseJsonNode != null) {
				responseArchiveFileRegistrationVO = objectMapper.convertValue(responseJsonNode, ArchiveFileRegistrationVO.class);
				Logger.debug("IndexDetailsUpdationBatchJob - updateAIMSIndexFileDetails() - responseArchiveFileRegistrationVO" +responseArchiveFileRegistrationVO.getAimsGuid()+" "+responseArchiveFileRegistrationVO.getIndexFileSize());
				regEntry.setStatus(Constant.AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE);
				regEntry.save();
			}else
			{
				Logger.warn("IndexDetailsUpdationBatchJob - updateAIMSRegistrationMasterInfo() -Response from AIMS is null for : "+regEntry.getAimsGuid());
			}
			
		}catch(Exception e)
		{
			Logger.error("IndexDetailsUpdationBatchJob - exception occurred while processing", e);
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
					Logger.warn("IndexDetailsUpdationBatchJob - Trigger/Key is null");
					return "IndexDetailsUpdationBatchJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("IndexDetailsUpdationBatchJob - scheduler is null");
				return "IndexDetailsUpdationBatchJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("IndexDetailsUpdationBatchJob - Exception in unscheduling the job "+e);
			return "IndexDetailsUpdationBatchJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("IndexDetailsUpdationBatchJob - scheduler is already running");
				return "IndexDetailsUpdationBatchJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("IndexDetailsUpdationBatchJob - Exception in rescheduling the job "+e);
			return "IndexDetailsUpdationBatchJob "+e.getMessage();
		}
	}
	
		
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("IndexDetailsUpdationBatchJob"))
			{
				Logger.warn("IndexDetailsUpdationBatchJob - Job is not allowed to run on this host ");
				return;
			}
			updateIndexDetails();
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
	}
}
