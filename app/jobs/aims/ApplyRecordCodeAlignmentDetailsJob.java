package jobs.aims;


import jobs.util.JobsUtil;
import models.storageapp.AppConfigProperty;
import models.storageapp.RCACycle;

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

public class ApplyRecordCodeAlignmentDetailsJob implements Job{

	private static Scheduler scheduler = null;
	private static final String ApplyRecordCodeAlignmentDetailsJobTrigger = "ApplyRecordCodeAlignmentDetailsJobTrigger";
	private static CronTrigger trigger = null;
	private static String ait;
	private static String projectId;
	private static String mode;
	
	
	public static void start() {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ApplyRecordCodeAlignmentDetailsJob"))
				return;
			Logger.info("ApplyRecordCodeAlignmentDetailsJob - starting the batch job at "+DateUtil.getCurrentDate());
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.APPLY_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR);
	   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
	   			Logger.error("ApplyRecordCodeAlignmentDetailsJob - "+Constant.APPLY_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR + " not found - skipping the iteration");
	   			return;
	   		}
	   			
			JobDetail jobDetail = JobBuilder.newJob(ApplyRecordCodeAlignmentDetailsJob.class).withIdentity(new JobKey("ApplyRecordCodeAlignmentDetailsJob")).build();
			trigger = TriggerBuilder.newTrigger()
						.withIdentity(ApplyRecordCodeAlignmentDetailsJobTrigger, Constant.RETENTION_UPDATE_JOBS)
					    .withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();

			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("ApplyRecordCodeAlignmentDetailsJob - Failed to schedule job", e);
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
					Logger.warn("ApplyRecordCodeAlignmentDetailsJob - Trigger/Key is null");
					return "ApplyRecordCodeAlignmentDetailsJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("ApplyRecordCodeAlignmentDetailsJob - scheduler is null");
				return "ApplyRecordCodeAlignmentDetailsJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("ApplyRecordCodeAlignmentDetailsJob - Exception in unscheduling the job "+e);
			return "ApplyRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("ApplyRecordCodeAlignmentDetailsJob - scheduler is already running");
				return "ApplyRecordCodeAlignmentDetailsJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("ApplyRecordCodeAlignmentDetailsJob - Exception in rescheduling the job "+e);
			return "ApplyRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	public static String runOnDemand(String aitParam, String projectIdParam, String modeParam){
		try
		{
			getRequestParameter(aitParam,projectIdParam,modeParam);
			String status = analyzeOrApplyRCAUpdates(Constant.ONDEMAND_RUN);
			if("".equalsIgnoreCase(status) || null == status){
				return Constant.SUCCESS;	
			}else{
				return Constant.SUCCESS_WITH_WARNING+" -"+status;
			}

		}catch(Exception e)
		{
			Logger.error("ApplyRecordCodeAlignmentDetailsJob - Exception in running the job on demand "+e);
			return "ApplyRecordCodeAlignmentDetailsJob "+e.getMessage();
		}
	}
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ApplyRecordCodeAlignmentDetailsJob"))
			{
				Logger.warn("ApplyRecordCodeAlignmentDetailsJob - Job is not allowed to run on this host ");
				return;
			}
			ait = "all";
			projectId = "all";
			mode = getModeToRun();
			
			analyzeOrApplyRCAUpdates(Constant.SCHEDULED_RUN);
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
			e.printStackTrace();
		}
		
	}
	
	public static void getRequestParameter(String aitParam, String projectIdParam, String modeParam){
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
		
		if(modeParam != null || !"".equals(modeParam)) {
			mode = modeParam;
		}else{
			mode = null;
		}
		
		if(ait == null && projectId == null && mode == null){
			ait = "all";
			projectId = "all";
			mode = getModeToRun();
		}
		if(mode == null || "".equals(mode)){
			Logger.warn("no mode provided to run the job...determine based on current cycle status");
			mode = getModeToRun();
		}
	}
	
	public static String getModeToRun(){
		// check the rca_cycle table to determine the current status and decide which mode it needs to run as 
		RCACycle currentCycle = RCACycle.getCurrentCycleDetails();
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

	public static String analyzeOrApplyRCAUpdates(String executionMode) throws Exception{
		String status = null;
		if("analysis".equalsIgnoreCase(mode)){
			status = AnalyzeRecordCodeAlignmentDetails.analyzeRecordCodeAlignmentDetails(executionMode,ait,projectId);
		}else if("update".equalsIgnoreCase(mode)){
			status = ApplyRecordCodeAlignmentDetails.applyRecordCodeAlignmentDetails(executionMode,ait,projectId);
		}
		return status;
	}
	
}
