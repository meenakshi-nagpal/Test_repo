package jobs.aims;

import java.sql.Timestamp;
import java.util.List;

import jobs.util.JobsUtil;
import models.storageapp.AppConfigProperty;
import models.storageapp.CandidatesFileId;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import play.Logger;
//import play.Play;
//import play.libs.Akka;
//import scala.concurrent.duration.Duration;
import utilities.Constant;
import utilities.DateUtil;
import valueobjects.CandidatesFileIdMasterVO;
import ws.AIMSWSClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@DisallowConcurrentExecution
public class CandidatesFileIdMasterJob implements Job {
	private static Scheduler scheduler = null;
	private static final String CandidatesFileIdMasterJobTrigger = "CandidatesFileIdMasterJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
	    try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
	    	Logger.info("CandidatesFileIdMasterJob - starting the batch job at "+DateUtil.getCurrentDate());
	    	AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.CANDIDATE_FILE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("CandidatesFileIdMasterJob - "+Constant.CANDIDATE_FILE_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   		
	    	JobDetail jobDetail = JobBuilder.newJob(CandidatesFileIdMasterJob.class).withIdentity(new JobKey("CandidatesFileIdMasterJob")).build(); 
	    	trigger = TriggerBuilder.newTrigger()
	    			.withIdentity(CandidatesFileIdMasterJobTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
	    			.withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			Logger.error("CandidatesFileIdMasterJob - exception occurred while scheduling job", e);
			e.printStackTrace();
		}

	}

	private void checkCandidatesFileId() {
		
		try {
			// getting url connection string from db
			Logger.debug("CandidatesFileIdMasterJob - checkCandidatesFileId() - starting to iteration "+DateUtil.getCurrentDate());
		    String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		    String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_CANDIDATE_FILE_ID_MASTER_URL_KEY);
		    String sourceId = AppConfigProperty.getAppConfigValue("UDAS_SOURCE_SYSTEM_ID");
		    						    	
		    if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
		    {
		    	Logger.error("CandidatesFileIdMaster job Failed - "
						+ "ReadycandidateFileIdList service URL not found in App Config.");
				return;
		    }

	    	String aimsUrl = root + extention.replace("{sourceSystem}", sourceId);
			
			JsonNode aimsJsonNode = AIMSWSClient.jerseyGetAIMSJsonResponse(aimsUrl);

			if(aimsJsonNode == null) {
				Logger.info("CandidatesFileIdMasterJob - Received null reponse from service.");
				return ;
			}
				
			ObjectMapper objectMapper = new ObjectMapper();
			
			List<CandidatesFileIdMasterVO> voList = 
					objectMapper.convertValue(aimsJsonNode, 
							new TypeReference<List<CandidatesFileIdMasterVO>>() {});
			
			for(CandidatesFileIdMasterVO candidatesFileIdMasterVO : voList) {
				if(CandidatesFileId.findById(
						candidatesFileIdMasterVO.getCandidatesFileId()) != null) {
					continue;
				}
				CandidatesFileId candidatesFileId =
						new CandidatesFileId();
				candidatesFileId.setId(candidatesFileIdMasterVO.getCandidatesFileId());
				candidatesFileId.setExpirationDate(DateUtil.parseDateYYYY_MM_DD(
						candidatesFileIdMasterVO.getExpirationDate()));
				candidatesFileId.setProcessStatus(
						Constant.CANDIDATES_PROCESS_STATUS_READY);
				candidatesFileId.setCreationDate(new Timestamp(System.currentTimeMillis()));
				candidatesFileId.save();
			}
			
		} catch (Exception e) {
			Logger.error("CandidatesFileIdMasterJob - exception occurred while processing", e);
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
					Logger.warn("CandidatesFileIdMasterJob - Trigger/Key is null");
					return "CandidatesFileIdMasterJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("CandidatesFileIdMasterJob - scheduler is null");
				return "CandidatesFileIdMasterJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("CandidatesFileIdMasterJob - Exception in unscheduling the job "+e);
			return "CandidatesFileIdMasterJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("CandidatesFileIdMasterJob - scheduler is already running");
				return "CandidatesFileIdMasterJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("CandidatesFileIdMasterJob - Exception in rescheduling the job "+e);
			return "CandidatesFileIdMasterJob "+e.getMessage();
		}
	}
	
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		if(!JobsUtil.isJobAllowedToRunOnThisHost("CandidatesFileIdMasterJob"))
			return;
		checkCandidatesFileId();		
	}

}
