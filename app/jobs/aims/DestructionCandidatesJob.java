package jobs.aims;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;

import jobs.util.JobsUtil;

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

import models.storageapp.AppConfigProperty;
import models.storageapp.CandidatesFileId;
import models.storageapp.DestructionCandidateKey;
import models.storageapp.DestructionCandidates;
import play.Logger;
import utilities.Constant;
import ws.AIMSWSClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@DisallowConcurrentExecution
public class DestructionCandidatesJob implements Job {
	
	private static Scheduler scheduler = null;
	private static final String DestructionCandidatesJobTrigger = "DestructionCandidatesJobTrigger";
	private static CronTrigger trigger = null;
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.DESTRUCTION_CANDIDATES_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("DestructionCandidatesJob - "+Constant.DESTRUCTION_CANDIDATES_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   		
			JobDetail jobDetail = JobBuilder.newJob(DestructionCandidatesJob.class).withIdentity(new JobKey("DestructionCandidatesJob")).build(); 
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(DestructionCandidatesJobTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			Logger.error("DestructionCandidatesJob - Error while scheduling job", e);
		}

	}

	private void checkDestructionCandidates() {
		long startTime = System.currentTimeMillis();
		Long jobId = System.currentTimeMillis();
		try {
			Logger.info("DestructionCandidatesJob - Starting processing CandidatesFileIds; Job ID -" + jobId);

			// getting url connection string from db
			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(Constant.AIMS_DESTRUCTION_LIST_URL_KEY);
			String sourceId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);

			if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty()) {
				Logger.error("DestructionCandidatesJob job Failed - "
						+ "Destruction List service URL not found in App Config.");
				Logger.debug("DestructionCandidatesJob - Stopping processing " +
						"CandidatesFileIds; Job ID -" + jobId);
				return;
			}

			String aimsUrl = root + extention.replace("{sourceSystem}", sourceId);

			Logger.debug("DestructionCandidatesJob - checking CandidatesFileIds in DB; Job ID -" + jobId);
			List<CandidatesFileId> listReadyCandidateFiles = CandidatesFileId
					.getCandidatesFileList(Constant.CANDIDATES_PROCESS_STATUS_READY);

			if(listReadyCandidateFiles == null || listReadyCandidateFiles.isEmpty()){
				Logger.info("DestructionCandidatesJob - No candidates available to process.");
				Logger.debug("DestructionCandidatesJob - Stopping processing " +
						"CandidatesFileIds; Job ID -" + jobId);
				return;
			}
			
			Logger.info("DestructionCandidatesJob - checked CandidatesFileIds in DB;" +
					" Job ID -" + jobId +
					" No. of Candidate file Ids to be processed: " + 
					listReadyCandidateFiles.size());
			
			int recordCounter = 0;
			int recordLimit = Integer.parseInt(AppConfigProperty.getAppConfigValue(Constant.DESTRUCTION_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_KEY));
			if(recordLimit < 1)
				recordLimit = Constant.DESTRUCTION_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_DEFAULT;
			
			for (CandidatesFileId candidatesFile : listReadyCandidateFiles) {
				String url = aimsUrl  + candidatesFile.getId();

				Logger.debug("DestructionCandidatesJob - Starting processing CandidatesFileId - " + 
						candidatesFile.getId());
				Logger.debug("DestructionCandidatesJob - Processing CandidatesFileId - " + 
						candidatesFile.getId() + " Calling AIMS to get destrction list." );
				JsonNode aimsJsonNode = AIMSWSClient
						.jerseyGetAIMSJsonResponse(url);
				Logger.info("DestructionCandidatesJob - Processing CandidatesFileId - " + 
						candidatesFile.getId() + " Response Recieved from AIMS to get destrction list." );

				if(aimsJsonNode == null) {
					Logger.error("DestructionCandidatesJob - Received null response from service for candidate " + candidatesFile.getId());
					continue ;
				}

				ObjectMapper objectMapper = new ObjectMapper();

				List<String> destructionList = objectMapper.convertValue(
						aimsJsonNode,
						new TypeReference<List<String>>() {
						});

				if(destructionList == null || destructionList.isEmpty()) {
					Logger.debug("DestructionCandidatesJob - Processing CandidatesFileId - " + 
							candidatesFile.getId() + " Conversion of JSON to List complete" +
							" No. of Records " + 0);
					continue;
				}
				
				if(recordCounter > recordLimit - destructionList.size())
				{
					Logger.info("DestructionCandidatesJob - Record process limit reached at " + recordLimit + ".");
					Logger.debug("DestructionCandidatesJob - Service took >>" + (System.currentTimeMillis() - startTime) + " ms to process " + recordCounter + " records.");
					break;
				}

				Logger.info("DestructionCandidatesJob - Processing CandidatesFileId - " + 
						candidatesFile.getId() + " Conversion of JSON to List complete" +
						" No. of Records " + destructionList.size());
				
				CountDownLatch latch = new CountDownLatch(destructionList.size()); 
				ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				
				for (String aimsGuid : destructionList) {

					try {
						
						
						recordCounter++;
					    executor.execute(new DestructionCandidatesJob().new DestructionCandidateUpdate(aimsGuid,candidatesFile, latch));
						
					} catch (Exception e) {
						Logger.error("DestructionCandidatesJob error - "
								+ "Error occured while processing destruction list " + aimsGuid + ". " + e.getMessage());						
						Logger.error("DestructionCandidatesJob - exception occurred while processing", e);
						e.printStackTrace();
						continue;
					}
				} //end of for loop

				 try {
				        latch.await();
				    } catch (InterruptedException e) {
				        e.printStackTrace();
				    }
				    Logger.debug("[DestructionCandidatesJob-job] - Shutting Down Executor Services ");
					executor.shutdown();
					Logger.debug("[DestructionCandidatesJob-job] - Shut down all service");
				
				Logger.debug("DestructionCandidatesJob - Updating CandidatesFileId - " + 
						candidatesFile.getId() + 
						" Setting New Status + " + Constant.CANDIDATES_PROCESS_STATUS_RECEIVED);
				
				candidatesFile
				.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_RECEIVED);
				candidatesFile.save();
				
				Logger.debug("DestructionCandidatesJob - Updated CandidatesFileId - " + 
						candidatesFile.getId() + 
						" New Status + " + Constant.CANDIDATES_PROCESS_STATUS_RECEIVED);

				Logger.info("DestructionCandidatesJob - Completed processing CandidatesFileId - " + 
						candidatesFile.getId());
			}
			Logger.info("DestructionCandidatesJob - Stopping processing CandidatesFileIds; Job ID -" + jobId + ". Processed " + recordCounter + " records in >> " + (System.currentTimeMillis() - startTime) + " ms. ");


		} catch (Exception e) {
			Logger.error("DestructionCandidatesJob error - "
					+ "Error occured while processing destruction list.", e);
			Logger.debug("DestructionCandidatesJob - Stopping processing CandidatesFileIds with Exception; Job ID -" + jobId);
		}

	}

	
	private class DestructionCandidateUpdate implements Runnable {

		String aimsGuid=null;
		CandidatesFileId candidatesFile;
		CountDownLatch latch ;
		public DestructionCandidateUpdate(String aimsGuid,CandidatesFileId candidatesFile,CountDownLatch latch){
		        this.aimsGuid = aimsGuid;
		        this.candidatesFile = candidatesFile;
		        this.latch = latch;
		 }
		
		
		@Override
		public void run() {
			
			try{
				DestructionCandidates destructionCandidate = new DestructionCandidates();

				DestructionCandidateKey destructionCandidateKey = new DestructionCandidateKey(
						candidatesFile.getId(), aimsGuid);

				destructionCandidate
				.setDestructionCandidateKey(destructionCandidateKey);


				destructionCandidate.setIsDeleted('N');


				destructionCandidate.save();
				Logger.debug("DestructionCandidatesJob - Processing CandidatesFileId - " + 
						candidatesFile.getId() + " Inserted into DESTRUCTION_CANDIDATE_LIST Table " +
						" AIMS GUID " + aimsGuid);	
			}//end of try
			catch (Exception e) {
				Logger.error("DestructionCandidatesJob error - Error occured while inserting record, aimsGuid:  " + aimsGuid + ". " + e.getMessage());						
				Logger.error("DestructionCandidatesJob - exception occurred while inserting records in Destuction Candidate List", e);
				e.printStackTrace();
			}
			finally{
				latch.countDown();
			}
			
			
		}
		
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("DestructionCandidatesJob - scheduler is already running");
				return "DestructionCandidatesJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("DestructionCandidatesJob - Exception in rescheduling the job "+e);
			return "DestructionCandidatesJob "+e.getMessage();
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
					Logger.warn("DestructionCandidatesJob - Trigger/Key is null");
					return "DestructionCandidatesJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("DestructionCandidatesJob - scheduler is null");
				return "DestructionCandidatesJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("DestructionCandidatesJob - Exception in unscheduling the job "+e);
			return "DestructionCandidatesJob "+e.getMessage();
		}
	}
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		if(!JobsUtil.isJobAllowedToRunOnThisHost("DestructionCandidatesJob"))
			return;
		checkDestructionCandidates();		
	}

}
