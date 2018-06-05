package jobs.aims;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

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
import models.storageapp.DestructionCandidates;
import play.Logger;
import play.mvc.Http;
//import play.libs.Akka;
//import scala.concurrent.duration.Duration;
import utilities.Constant;
import ws.AIMSWSClient;
import akka.actor.Cancellable;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

@DisallowConcurrentExecution
public class ProcessExpiredCandidatesScheduler implements Job {
		private volatile static boolean isProcessing = false;
		private static Scheduler scheduler = null;
		private static final String ProcessTrigger = "ProcessExpiredCandidatesSchedulerTrigger";
		private static CronTrigger trigger = null;

		public static void schedule() {
		    try {
				if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
					return;
		    	AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.PROCESS_EXPIRED_BATCH_JOB_CRON_EXPR);
		   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
		   			Logger.error("processExpiredCandidate-job - "+Constant.PROCESS_EXPIRED_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
		   			return;
		   		}
		   		
		    	JobDetail jobDetail = JobBuilder
		    			.newJob(ProcessExpiredCandidatesScheduler.class)
		    		    .withIdentity(
		    					new JobKey("ProcessExpiredCandidatesScheduler"))
		    			.build(); 
		    	trigger = TriggerBuilder.newTrigger()
						.withIdentity(ProcessTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

				scheduler = StdSchedulerFactory.getDefaultScheduler();
				scheduler.scheduleJob(jobDetail, trigger);
			} catch (SchedulerException e) {
				Logger.error("ProcessExpiredCandidatesScheduler - exception occurred while scheduleing job", e);
				e.printStackTrace();
			}
		    
		}

		public static String startTrigger() {		
			try
			{
				if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
				{
					Logger.warn("ProcessExpiredCandidatesScheduler - scheduler is already running");
					return "ProcessExpiredCandidatesScheduler - "+Constant.SCHEDULER_ALREADY_RUNNING;
				}else{
					schedule();
					return Constant.SUCCESS;
				}
			}catch(Exception e)
			{
				Logger.error("ProcessExpiredCandidatesScheduler - Exception in rescheduling the job "+e);
				return "ProcessExpiredCandidatesScheduler "+e.getMessage();
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
						Logger.warn("ProcessExpiredCandidatesScheduler - Trigger/Key is null");
						return "ProcessExpiredCandidatesScheduler - "+Constant.TRIGGER_NOT_FOUND;
					}
				}
				else{
					Logger.warn("ProcessExpiredCandidatesScheduler - scheduler is null");
					return "ProcessExpiredCandidatesScheduler - "+Constant.SCHEDULER_NOT_FOUND;
				}
			}catch(Exception e)
			{
				Logger.error("ProcessExpiredCandidatesScheduler - Exception in unscheduling the job "+e);
				return "ProcessExpiredCandidatesScheduler "+e.getMessage();
			}
		}
		
		private void runJob() {
			if (isProcessing) {
				Logger.info("[processExpiredCandidate-job] - Already running jobs");
			} else {
				try {
					isProcessing = true;
					processExpiredCandidate();
					isProcessing = false;
				} finally {
					isProcessing = false;
				}
			}
		}

		private static void processExpiredCandidate() {

			List<CandidatesFileId> candidateFilelist = CandidatesFileId
					.getExpiredCandidatesFiles();
			if (candidateFilelist != null && !candidateFilelist.isEmpty()) {
				Logger.debug("[processExpiredCandidate-job] - Number of candidate file(s) found: " + candidateFilelist.size());
				
		    // getting url connection string from db
		    String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		    String extention = AppConfigProperty.getAppConfigValue(Constant.DELETE_CONFIRMATION_SERVICE_URL);
		    String sourceId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);

		    if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
		    {
		    	Logger.error("[processExpiredCandidate-job] - ProcessExpiredCandidates job Failed - "
						+ "Delete confirmation service URL not found in App Config.");
				return;
		    }
		    
		    String url = root + extention.replace("{sourceSystem}", sourceId);
		    
				for (CandidatesFileId candidateFile : candidateFilelist) {
					try {
						List<DestructionCandidates> candidatelist = DestructionCandidates
								.getCandidateFiles(candidateFile.getId());
						
						if (candidatelist != null && !candidatelist.isEmpty()) {
							
							Logger.debug("[processExpiredCandidate-job] - Destruction item(s) found:" + candidatelist.size() + " for candidate: " + candidateFile.getId());

						    	String aimsUrl = url + candidateFile.getId();

								// Compose the Request for AIMS service
								JsonNode requestBody = createRequestBody(candidatelist);
						    	
								if (requestBody == null) {
									Logger.error("[processExpiredCandidate-job] - " + candidateFile.getId()
											+ " cannot form request for Delete Confirmation service .");
									continue;
								}
								
								/* Getting issue with Play ws client with POST
								 * JsonNode response = AIMSWSClient
										.postToAIMSAndGetJsonResponse(aimsUrl,
												requestBody);*/
								
								int response = AIMSWSClient.jerseyPostToAIMSAndGetStatus(
										aimsUrl, requestBody);								

								// Check response for validity
								if (response == Http.Status.OK) {						
									candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PROCESSED);
									candidateFile.update();
								}
								else
								{
									Logger.info("[processExpiredCandidate-job] - Aims confirmation service returned with " + response + " response.");
								}
							}
					} catch (Exception e) {
						Logger.error("[processExpiredCandidate-job] - " 
								+ " Error occurred processing candidate " + candidateFile.getId());
						Logger.error("ProcessExpiredCandidatesScheduler - exception occurred while processing", e);
						e.printStackTrace();
						continue;
					}
				}// end candidate for loop
			}

		}
		
		public static JsonNode createRequestBody(List<DestructionCandidates> list)
		{
			JsonNode jsonNode = null;
			final OutputStream out = new ByteArrayOutputStream();
			final ObjectMapper mapper = new ObjectMapper();
	 
			SimpleModule module = new SimpleModule();
			module.addSerializer(DestructionCandidates.class, new DestructionCandidateSerializer());
			mapper.registerModule(module);
		
			try {
				mapper.writeValue(out, list);
				final byte[] data = ((ByteArrayOutputStream) out).toByteArray();
				jsonNode = mapper.readTree(data);
				return jsonNode;
			} catch (JsonGenerationException e) {
				Logger.error("ProcessExpiredCandidatesScheduler - exception occurred while " +
						"JSON Generation", e);
				e.printStackTrace();
			} catch (JsonMappingException e) {
				Logger.error("ProcessExpiredCandidatesScheduler - exception occurred while " +
						"JSON Mapping", e);
				e.printStackTrace();
			} catch (IOException e) {
				Logger.error("ProcessExpiredCandidatesScheduler - exception occurred while " +
						"JSON Serializtion", e);
				e.printStackTrace();
			}

			return jsonNode;
		}

		@Override
		public void execute(JobExecutionContext arg0)
				throws JobExecutionException {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ProcessExpiredCandidatesScheduler"))
				return;
			runJob();
		}
	}

		  

