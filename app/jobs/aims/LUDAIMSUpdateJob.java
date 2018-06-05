package jobs.aims;

import java.util.Date;
import java.util.List;

import jobs.util.JobsUtil;
import models.storageapp.AccessData;
import models.storageapp.AppConfigProperty;

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
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import adapters.aims.AimsAdapter;
import adapters.aims.UdasLudEntity;

import play.Logger;
import utilities.Constant;
import utilities.DateUtil;

@DisallowConcurrentExecution
public class LUDAIMSUpdateJob implements Job {

	private static final String LUD_UPDATE_JOB = "LUDAIMSUpdateJob";
	private static final String EPOCH_START = "1970-01-01";

	private static Scheduler scheduler = null;
	private static CronTrigger trigger = null;

	private Integer processedCount;

	private static final String LudTrigger = "LudUpdateBatchJobTrigger";

	public static void start() {

		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost(LUD_UPDATE_JOB))
				return;
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(
					Constant.LUD_UPDATE_BATCH_JOB_CRON_EXPR);
			if(appConfigProperty == null || appConfigProperty.getValue() == null || 
					appConfigProperty.getValue().trim().isEmpty()) {
				Logger.error(LUD_UPDATE_JOB + " - "+ 
						Constant.LUD_UPDATE_BATCH_JOB_CRON_EXPR + 
						" not found - " +  LUD_UPDATE_JOB + " will NOT run.");
				return;
			}

			JobDetail jobDetail = JobBuilder
					.newJob(LUDAIMSUpdateJob.class)
					.withIdentity(
							new JobKey(LUD_UPDATE_JOB))
							.build(); 
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(LudTrigger,Constant.DEFENSIBLE_DISPOSITION_JOBS)
					.withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error(LUD_UPDATE_JOB + " - Failed to schedule job", e);
		}
	}


	private void setProcessCount(int newValue) {
		if(processedCount != null) {
			processedCount = newValue;
		}
	}

	private int getProcessedCount() {
		if(processedCount != null) {
			return processedCount;
		}
		return 0;
	}

	private void updateLUDsOnAIMS() {

		Date rightNow = new Date(System.currentTimeMillis());
		String todaysDateString = DateUtil.formatDateYYYY_MM_DD(rightNow);

		try {
			Date fromDate = null, toDate = null;

			AppConfigProperty ludFromDateProp = AppConfigProperty.getPropertyByKey(
					Constant.LUD_JOB_LAST_RUN_DATE);

			if(ludFromDateProp == null || ludFromDateProp.getValue() == null ||
					ludFromDateProp.getValue().isEmpty()) {
				fromDate = DateUtil.parseDateYYYY_MM_DD(EPOCH_START);
			} else {
				fromDate = DateUtil.parseDateYYYY_MM_DD(ludFromDateProp.getValue());
			}

			toDate = DateUtil.parseDateYYYY_MM_DD(todaysDateString);

			AccessData accessData = new AccessData();

			List<UdasLudEntity> udasLudEntities = accessData.retrieveLUDsFromDateToToDate(
					fromDate, toDate);

			if(udasLudEntities == null || udasLudEntities.isEmpty())
				return;

			AimsAdapter aimsAdapter = AimsAdapter.getInstance();

			int[] resultList =  aimsAdapter.updateUDASLudsOnAIMSDB(udasLudEntities);
			int total = 0;
			for(int i : resultList) {
				total += i;
			}

			setProcessCount(total);
		} catch (Exception e) {
			Logger.error(LUD_UPDATE_JOB + " - Exception in updating LUDs " + e);
			e.printStackTrace();
		} finally {

			AppConfigProperty ludLastRunDateProp = AppConfigProperty.getPropertyByKey(
					Constant.LUD_JOB_LAST_RUN_DATE);
			if(ludLastRunDateProp == null) {
				ludLastRunDateProp = new AppConfigProperty();
				ludLastRunDateProp.setKey(Constant.LUD_JOB_LAST_RUN_DATE);
			}
			ludLastRunDateProp.setValue(todaysDateString);
			ludLastRunDateProp.save();
		}
	}

	public static String startTrigger() {
		try {
			if (scheduler != null && trigger != null && trigger.getKey() != null && 
					scheduler.getTriggerState(trigger.getKey()).equals(
							Trigger.TriggerState.NORMAL)) {
				Logger.warn(LUD_UPDATE_JOB + " - scheduler is already running");
				return LUD_UPDATE_JOB + " - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		} catch(Exception e) {
			Logger.error(LUD_UPDATE_JOB + " - Exception in rescheduling the job "+e);
			return LUD_UPDATE_JOB + " "+e.getMessage();
		}
	}

	public static String stopTrigger() {
		try {
			if (scheduler != null) {
				if (trigger != null && trigger.getKey() != null) {
					scheduler.unscheduleJob(trigger.getKey());
					return Constant.SUCCESS;
				} else {
					Logger.warn(LUD_UPDATE_JOB + " - Trigger/Key is null");
					return LUD_UPDATE_JOB + " - " + Constant.TRIGGER_NOT_FOUND;
				}
			}
			else {
				Logger.warn(LUD_UPDATE_JOB + " - scheduler is null");
				return LUD_UPDATE_JOB + " - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e) {
			Logger.error(LUD_UPDATE_JOB + " - Exception in unscheduling the job "+e);
			return LUD_UPDATE_JOB + " "+e.getMessage();
		}
	}

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost(LUD_UPDATE_JOB))
				return;
			final long iterationId = System.currentTimeMillis();
			Logger.info(LUD_UPDATE_JOB + " Starting iteration - " + 
					iterationId);
			processedCount = new Integer(0);
			updateLUDsOnAIMS();
			Logger.info(LUD_UPDATE_JOB + " Stopping iteration - " + 
					iterationId + ": " + getProcessedCount() + " accounts LUDs updated.");
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
		}
	}

}
