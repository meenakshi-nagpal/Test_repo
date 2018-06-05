package jobs.aims;


import jobs.util.JobsUtil;
import models.storageapp.AppConfigProperty;
import models.storageapp.RetentionCycle;
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

public class ApplyRetentionUpdatesJob implements Job{

	private static Scheduler scheduler = null;
	private static final String ApplyRetentionUpdatesJobTrigger = "ApplyRetentionUpdatesJobTrigger";
	private static CronTrigger trigger = null;
	private static String ait;
	private static String projectId;
	private static String recordCode;
	private static String mode;
	
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ApplyRetentionUpdatesJob"))
				return;
			Logger.info("ApplyRetentionUpdatesJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.APPLY_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("ApplyRetentionUpdatesJob - "+Constant.APPLY_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(ApplyRetentionUpdatesJob.class).withIdentity(new JobKey("ApplyRetentionUpdatesJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(ApplyRetentionUpdatesJobTrigger, Constant.RETENTION_UPDATE_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("ApplyRetentionUpdatesJob - Failed to schedule job", e);
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
					Logger.warn("ApplyRetentionUpdatesJob - Trigger/Key is null");
					return "ApplyRetentionUpdatesJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("ApplyRetentionUpdatesJob - scheduler is null");
				return "ApplyRetentionUpdatesJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("ApplyRetentionUpdatesJob - Exception in unscheduling the job "+e);
			return "ApplyRetentionUpdatesJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("ApplyRetentionUpdatesJob - scheduler is already running");
				return "ApplyRetentionUpdatesJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("ApplyRetentionUpdatesJob - Exception in rescheduling the job "+e);
			return "ApplyRetentionUpdatesJob "+e.getMessage();
		}
	}
	
	public static String runOnDemand(String aitParam, String projectIdParam, String recordCodeParam, String modeParam){
		try
		{
			getRequestParameter(aitParam,projectIdParam,recordCodeParam,modeParam);
			String status = analyzeOrApplyRetentionUPdates(Constant.ONDEMAND_RUN);
			if("".equalsIgnoreCase(status) || null == status){
				return Constant.SUCCESS;	
			}else{
				return Constant.SUCCESS_WITH_WARNING+" -"+status;
			}

		}catch(Exception e)
		{
			Logger.error("ApplyRetentionUpdatesJob - Exception in running the job on demand "+e);
			return "ApplyRetentionUpdatesJob "+e.getMessage();
		}
	}
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ApplyRetentionUpdatesJob"))
			{
				Logger.warn("ApplyRetentionUpdatesJob - Job is not allowed to run on this host ");
				return;
			}
			ait = "all";
			projectId = "all";
			recordCode = "all";
			mode = getModeToRun();
			
			analyzeOrApplyRetentionUPdates(Constant.SCHEDULED_RUN);
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
		
	}
	
	public static void getRequestParameter(String aitParam, String projectIdParam, String recordCodeParam, String modeParam){
		if(aitParam != null || !"".equals(aitParam)) {
			ait = aitParam;
		}else{
			ait = null;
		}
		
		if(projectIdParam != null || !"".equals(projectIdParam)) {
			projectId = projectIdParam;
		}else{
			projectId = null;
		}
		
		if(recordCodeParam != null || !"".equals(recordCodeParam)) {
			recordCode = recordCodeParam;
		}else{
			recordCode = null;
		}
		
		if(modeParam != null || !"".equals(modeParam)) {
			mode = modeParam;
		}else{
			mode = null;
		}
		
		if(ait == null && projectId == null && recordCode == null && mode == null){
			ait = "all";
			projectId = "all";
			recordCode = "all";
			mode = getModeToRun();
		}
		if(mode == null || "".equals(mode)){
			Logger.warn("no mode provided to run the job...determine based on current cycle status");
			mode = getModeToRun();
		}
	}
	
	public static String getModeToRun(){
		// check the retention_cycle table to determine the current status and decide which mode it needs to run as 
		RetentionCycle currentCycle = RetentionCycle.getCurrentCycleDetails();
		if(currentCycle != null){
			if(currentCycle.getCurrentStatus() >= Constant.CYCLE_STATUS_CYCLE_STARTED && currentCycle.getCurrentStatus() <= Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED){
				return "analysis";
			}else if(currentCycle.getCurrentStatus() >= Constant.CYCLE_STATUS_ANALYSIS_VALIDATED){
				return "update";
			}
		}else{
			Logger.warn("No cycle available");
		}
		return null;
	}

	public static String analyzeOrApplyRetentionUPdates(String executionMode) throws Exception{
		String status = null;
		if("analysis".equalsIgnoreCase(mode)){
			status = AnalyzeRetentionUpdates.analyzeRetentionUpdates(executionMode,ait,projectId,recordCode);
		}else if("update".equalsIgnoreCase(mode)){
			status = ApplyRetentionUpdates.applyRetentionUpdates(executionMode,ait,projectId,recordCode);
		}
		return status;
	}
	
}
