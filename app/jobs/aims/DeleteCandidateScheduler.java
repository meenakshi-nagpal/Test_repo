package jobs.aims;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;


import jobs.util.JobsUtil;
import models.storageapp.AIMSRegistration;
import models.storageapp.AppConfigProperty;
import models.storageapp.CandidatesFileId;
import models.storageapp.DestructionCandidates;
import java.util.concurrent.BrokenBarrierException;
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
import play.mvc.Http;
import utilities.Constant;
import utilities.Utility;
import ws.AIMSWSClient;
import akka.actor.Cancellable;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import controllers.MetadataController;

@DisallowConcurrentExecution
public class DeleteCandidateScheduler implements Job {

	
	private volatile static boolean isProcessing = false;
	private static Scheduler scheduler = null;
	private static final String DeleteTrigger = "DeleteCandidateSchedulerTrigger";
	private static CronTrigger trigger = null;
	static volatile  boolean  isErrorInBatch = false;
	
	public static void schedule() {
	    try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
	    	AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.DELETE_CANDIDATE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("DeleteCandidateScheduler-job - "+Constant.DELETE_CANDIDATE_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   		
	    	JobDetail jobDetail = JobBuilder.newJob(DeleteCandidateScheduler.class).withIdentity(new JobKey("DeleteCandidateScheduler")).build(); 
	    	trigger = TriggerBuilder.newTrigger()
					.withIdentity(DeleteTrigger, Constant.DEFENSIBLE_DISPOSITION_JOBS)
				    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

	    	scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			Logger.error("DeleteCandidateScheduler - exception occurred while scheduling job", e);
			e.printStackTrace();
		}
	}

	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("DeleteCandidateScheduler - scheduler is already running");
				return "DeleteCandidateScheduler - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				schedule();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("DeleteCandidateScheduler - Exception in rescheduling the job "+e);
			return "DeleteCandidateScheduler "+e.getMessage();
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
					Logger.warn("DeleteCandidateScheduler - Trigger/Key is null");
					return "DeleteCandidateScheduler - "+Constant.SCHEDULER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("DeleteCandidateScheduler - scheduler is null");
				return "DeleteCandidateScheduler - "+Constant.TRIGGER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("DeleteCandidateScheduler - Exception in unscheduling the job "+e);
			return "DeleteCandidateScheduler "+e.getMessage();
		}
	}
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		if(!JobsUtil.isJobAllowedToRunOnThisHost("DeleteCandidateScheduler"))
			return;
		DeleteCandidateScheduler.runJob();		
	}
	
	
	private static void runJob() {
		if (isProcessing) {
			Logger.info("[DeleteCandidateScheduler-job] - Already running jobs");
		} else {
			try {
				isProcessing = true;
				processDeleteCandidate();
				isProcessing = false;
			} finally {
				isProcessing = false;
			}
		}
	}

	private static void processDeleteCandidate() {
		long startTime = System.currentTimeMillis();
		
		// Get all available Candidates to traverse
		List<CandidatesFileId> candidateFilelist = CandidatesFileId
				.getCandidatesFileToDelete();
		Logger.info("[DeleteCandidateScheduler-job] - startTime:  "+startTime+"--candidateFilelist.size()--"+candidateFilelist.size());
		if (candidateFilelist != null && !candidateFilelist.isEmpty()) {
			Logger.debug("[DeleteCandidateScheduler-job] - Number of candidates in deleteConfirmation job:" + candidateFilelist.size());
			
			// getting url connection string from db

		    String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
		    String extention = AppConfigProperty.getAppConfigValue(Constant.DELETE_CONFIRMATION_SERVICE_URL);
		    String sourceId = AppConfigProperty.getAppConfigValue(Constant.UDAS_SOURCE_SYSTEM_ID_KEY);
		    						    	
		    if(root.isEmpty() || extention.isEmpty() || sourceId.isEmpty())
		    {
		    	Logger.error("[DeleteCandidateScheduler-job] - "
						+ "Delete confirmation service URL not found in App Config.");
				return;
		    }

	    	String url = root + extention.replace("{sourceSystem}", sourceId);

			int recordCounter = 0;
			int recordLimit = Integer.parseInt(AppConfigProperty.getAppConfigValue(Constant.DELETE_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_KEY));
			if(recordLimit < 1)
				recordLimit = Constant.DELETE_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_DEFAULT;
			
			
			for (CandidatesFileId candidateFile : candidateFilelist) {
				isErrorInBatch = false;
				try {
					//Get available destruction list to delete for each candidate
					List<DestructionCandidates> candidatelist = DestructionCandidates
							.getReadyToDeleteFile(candidateFile.getId());

					if (candidatelist != null && !candidatelist.isEmpty()) {
						Logger.info("[DeleteCandidateScheduler-job] - Number of destruction items in deleteConfirmation jobs:" + candidatelist.size() + " candidates - " + candidateFile.getId());
						
						if(recordCounter > recordLimit - candidatelist.size())
						{
							Logger.info("[DeleteCandidateScheduler-job] - Record process limit reached at " + recordLimit + ".");
							Logger.debug("[DeleteCandidateScheduler-job] - Service took >>" + (System.currentTimeMillis() - startTime) + " ms to process " + recordCounter + " records.");
							break;
						}
						
					// Logger.debug("[DeleteCandidateScheduler-job] - recordCounter at " + recordCounter + ".");
							
						//Mark as locked
						candidateFile
								.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_LOCKED);
						candidateFile.update();
						
						CountDownLatch latch = new CountDownLatch(candidatelist.size()); 
						
						ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
						 
						 Logger.debug("[DeleteCandidateScheduler-job] - candidatelist.size()" + candidatelist.size());
					
						 Logger.debug("[DeleteCandidateScheduler-job] - recordCounter at " + recordCounter + ".");
				
						
						for (DestructionCandidates dispositionCandidate : candidatelist) {
							recordCounter++;
							
							Logger.debug("[DeleteCandidateScheduler-job] - Inside inner loop-" + dispositionCandidate );
							
							executor.execute(new DeleteCandidateScheduler().new DeleteCandidate(dispositionCandidate, latch));
							//end of code copied
						}//Inner destruction for loop
					
						
						 try {
						        
							       latch.await();
						    } catch (InterruptedException e) {
						        e.printStackTrace();
						    }
						   
							executor.shutdown();
							Logger.debug("[DeleteCandidateScheduler-job] - Shut down all service");
						
					}
					else
					{
						Logger.info("[DeleteCandidateScheduler-job] - No destruction item found to delete for candidates - " + candidateFile.getId());
					}
					Logger.debug("[DeleteCandidateScheduler-job] - Calling AIMS service to update AIMS isErrorInBatch - " + isErrorInBatch);
					//Call AIMS service to update AIMS.
					if (isErrorInBatch) { // if one error in deleting a record
												// we mark the batch as pending to
												// try this for next run
						Logger.debug("[DeleteCandidateScheduler-job] - Shut down all service isErrorInBatchsetting prcess status to pending - "+isErrorInBatch );
						candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PENDING);
						candidateFile.update();
					} else {// All files deleted successfully, and send
							// Confirmation msg right away and mark as
							// processed.

					    String aimsUrl = url + candidateFile.getId();

					    //get all destructionList 
					    List<DestructionCandidates> fullCandidatelist = DestructionCandidates
								.getCandidateFiles(candidateFile.getId());
					    Logger.debug("[DeleteCandidateScheduler-job] -fullCandidatelist - "+fullCandidatelist.size() );
					    if(fullCandidatelist != null && !fullCandidatelist.isEmpty())
					    {
							//Compose the Request for AIMS service
							Logger.debug("[DeleteCandidateScheduler-job] - Creating request body for delete confirmation service.");
							JsonNode requestBody = createRequestBody(fullCandidatelist);
							if (requestBody == null) {
								Logger.error("[DeleteCandidateScheduler-job] - " + candidateFile.getId()
										+ " cannot form request for Delete Confirmation service.");
								
								candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PENDING);
								candidateFile.update();
								continue;
							}

							Logger.info("[DeleteCandidateScheduler-job] - AIMS Delete confirmation service URL:" + aimsUrl);
							Logger.info("[DeleteCandidateScheduler-job] - AIMS Delete confirmation service REQUEST SEND.");

							int response = AIMSWSClient.jerseyPostToAIMSAndGetStatus(
									aimsUrl, requestBody);

							// Check response for validity
							if (response == Http.Status.OK) {
								candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PROCESSED);
								candidateFile.update();
								Logger.info("[DeleteCandidateScheduler-job] - Aims confirmation service returned with success for Candidate - " + candidateFile.getId());
							}
							else
							{
								Logger.info("[DeleteCandidateScheduler-job] - Aims confirmation service returned with " + response + " response.");
								candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PENDING);
								candidateFile.update();
							}
					   	}
					    
					    Logger.debug("[DeleteCandidateScheduler-job] - processed " + recordCounter + " records in >> " + (System.currentTimeMillis() - startTime) + " ms.");
					}
				} catch (Exception e) {
					Logger.info("[DeleteCandidateScheduler-job] - Error occured for candidate " + candidateFile.getId());
					Logger.error("DeleteCandidateScheduler - exception occurred while processing", e);
					e.printStackTrace();
					if(candidateFile.getProcessStatus() == Constant.CANDIDATES_PROCESS_STATUS_LOCKED)
					{
						candidateFile.setProcessStatus(Constant.CANDIDATES_PROCESS_STATUS_PENDING);
						candidateFile.update();
					}
					continue;
				}

			}// end outer candidate for loop
		}
	}
	
	
	private class DeleteCandidate implements Runnable {

		DestructionCandidates dispositionCandidate=null;
		CountDownLatch latch ;
		public DeleteCandidate(DestructionCandidates dispositionCandidate,CountDownLatch latch){
		        this.dispositionCandidate=dispositionCandidate;
		        this.latch = latch;
		 }
		
		
		@Override
		public void run() {
			
			
			try{
			String aimsGuid = dispositionCandidate
					.getDestructionCandidateKey().getAimsGuid();
			if (Utility.isNullOrEmpty(aimsGuid)) {
				Logger.error("[DeleteCandidateScheduler-job] - No GUID found in Disposition Candidate.-"+Thread.currentThread());
				dispositionCandidate
						.setFailureReasonText(Constant.ERROR_CODE_A);
				dispositionCandidate.setFailureReasonCode("A");
				dispositionCandidate.save();
				if (!isErrorInBatch)
					isErrorInBatch = true; Logger.debug("[DeleteCandidateScheduler-job] - isErrorInBatch 321- "+isErrorInBatch+"   "+Thread.currentThread());
				return;
			}

			//Get UDAS ID from AIMS Registaration table
			AIMSRegistration aimsInfo = AIMSRegistration
					.getUdasIdbyAimsGuid(aimsGuid);

			if (aimsInfo == null) {
				Logger.error("[DeleteCandidateScheduler-job] - No matching UDAS Id registered.- "+aimsGuid+"   "+Thread.currentThread());
				dispositionCandidate
						.setFailureReasonText(Constant.ERROR_CODE_A);
				dispositionCandidate.setFailureReasonCode("A");
				dispositionCandidate.save();
				if (!isErrorInBatch)
					isErrorInBatch = true;Logger.debug("[DeleteCandidateScheduler-job] - isErrorInBatch 336- "+isErrorInBatch);
				return;
			}
			
			String udasId = aimsInfo.getId();
			Logger.debug("[DeleteCandidateScheduler-job] - Matching UDAS id " + udasId + " found for AIMS ID " + aimsGuid+"--thread - "+Thread.currentThread());
			//Do the deletion
			Map<String, String> results = new HashMap<String, String>();
			if (!Utility.isNullOrEmpty(udasId)) {
				results = MetadataController
						.deleteArchiveForId(udasId);
			}

			//Check the deletion result and update the
			// DestructionCandidates
			if (results != null && !results.isEmpty()) {
				if ("ok".equalsIgnoreCase(results.get("text"))) {
					dispositionCandidate.setIsDeleted('Y');
					dispositionCandidate
							.setDeletionTimeStamp(new Date());
					dispositionCandidate.setFailureReasonCode("");
					dispositionCandidate.setFailureReasonText("");
					// persist
					Logger.debug("[DeleteCandidateScheduler-job] - Success: Archive " + udasId + " has been deleted successfully - "
							+ Utility.getCurrentTime()+"---Thread---"+Thread.currentThread());
					dispositionCandidate.save();
				} else {
					dispositionCandidate
							.setFailureReasonText(results
									.get("text"));
					dispositionCandidate
							.setFailureReasonCode(results
									.get("code"));
					dispositionCandidate.save();
					if (!isErrorInBatch)
						isErrorInBatch = true; Logger.debug("[DeleteCandidateScheduler-job] - isErrorInBatch 371- "+isErrorInBatch);
					return;
				}

			} else {
				Logger.error("[DeleteCandidateScheduler-job] - Unknown Error occurred during deletion process. "+aimsGuid+"   "+Thread.currentThread());
				dispositionCandidate
						.setFailureReasonText(Constant.ERROR_CODE_C);
				dispositionCandidate.setFailureReasonCode("C");
				if (!isErrorInBatch)
					isErrorInBatch = true;Logger.debug("[DeleteCandidateScheduler-job] - isErrorInBatch 381- "+isErrorInBatch);
				dispositionCandidate.save();
				return;
			}
			
			}
			catch (Exception e) {
				Logger.info("[DeleteCandidateScheduler-job] - Error occured for candidate Id " + dispositionCandidate.getDestructionCandidateKey());
				Logger.info("[DeleteCandidateScheduler-job] - Error for AIMS ID " + dispositionCandidate.getDestructionCandidateKey().getAimsGuid());
				Logger.error("DeleteCandidateScheduler - exception occurred while Deleting", e);
				e.printStackTrace();
			}
			finally{
				latch.countDown();
	        	
	        }
		} //end of run
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
			Logger.error("DeleteCandidateScheduler - exception occurred while Json Generation", e);
			e.printStackTrace();
		} catch (JsonMappingException e) {
			Logger.error("DeleteCandidateScheduler - exception occurred while Json Mapping", e);
			e.printStackTrace();
		} catch (IOException e) {
			Logger.error("DeleteCandidateScheduler - exception occurred while Json serializing", e);
			e.printStackTrace();
		}

		return jsonNode;
	}

}

class DestructionCandidateSerializer extends JsonSerializer<DestructionCandidates> {
    @Override
    public void serialize(DestructionCandidates candidate, JsonGenerator jgen, SerializerProvider provider) 
      throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeStringField("recordIdentifier", candidate.getDestructionCandidateKey().getAimsGuid());
        jgen.writeStringField("candidateListId",  candidate.getDestructionCandidateKey().getCandidateFileId());
        if(candidate.getIsDeleted() == 'Y')
        	jgen.writeNumberField("isDeleted", 1);
        else
        	jgen.writeNumberField("isDeleted", 0);
        	
        if(candidate.getDeletionTimeStamp() != null)
            jgen.writeStringField("destructionTimeStamp", new SimpleDateFormat("yyyy-MM-dd").format(candidate.getDeletionTimeStamp()));
        else
            jgen.writeStringField("destructionTimeStamp", "");

        jgen.writeStringField("failureReason", candidate.getFailureReasonCode());
        jgen.writeStringField("failureReasonDescription", candidate.getFailureReasonText());
        jgen.writeEndObject();
    }
	
}
