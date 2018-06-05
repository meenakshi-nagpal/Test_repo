package jobs.aims;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import jobs.util.JobsUtil;

import org.glassfish.jersey.client.ChunkedInput;
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

import models.storageapp.AIMSRegistration;
import models.storageapp.AppConfigProperty;
import models.storageapp.Helper;
import models.storageapp.Metadata;





import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;
import com.avaje.ebean.Query;
import com.avaje.ebean.QueryListener;
import com.avaje.ebean.RawSql;
import com.avaje.ebean.RawSqlBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import play.Logger;
import playutils.PropertyUtil;
import utilities.Constant;
import utilities.DateUtil;
import valueobjects.ArchiveFileRegistrationVO;
import ws.JerseyWSClient;

@DisallowConcurrentExecution
public class ArchiveRegistrationBatchJob implements Job {
	
	private static final int SLEEP_BATCH_COUNT = 30;
	private static final int SLEEP_MILLIS = 10;
	private static final String ArchiveTrigger = "ArchiveRegistrationBatchJobTrigger";
	
	private static Scheduler scheduler = null;
	private static CronTrigger trigger = null;
	
	private AtomicInteger processedCount;

	public static void start() {

		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
			AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(
					Constant.ARCHIVE_REGISTRATION_BATCH_JOB_CRON_EXPR);
			if(appConfigProperty == null || appConfigProperty.getValue() == null || 
					appConfigProperty.getValue().trim().isEmpty()) {
				Logger.error("ArchiveRegistrationBatchJob - "+ 
						Constant.ARCHIVE_REGISTRATION_BATCH_JOB_CRON_EXPR + 
						" not found - ArchiveRegistrationBatchJob will NOT run.");
				return;
			}

			JobDetail jobDetail = JobBuilder
					.newJob(ArchiveRegistrationBatchJob.class)
					.withIdentity(
							new JobKey("ArchiveRegistrationBatchJob"))
							.build(); 
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(ArchiveTrigger,Constant.DEFENSIBLE_DISPOSITION_JOBS)
							.withSchedule(CronScheduleBuilder.cronSchedule(appConfigProperty.getValue())).build();
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			Logger.error("ArchiveRegistrationBatchJob - Failed to schedule job", e);
		}
	}

	private int incrementProcessCount() {
		if(processedCount != null) {
			return processedCount.incrementAndGet();
		}
		return 0;
	}
	
	private int getProcessedCount() {
		if(processedCount != null) {
			return processedCount.get();
		}
		return 0;
	}

	private boolean processIncompleteRegistrations(final Object syncObject, 
			final long iterationId) {

		Map<String, BufferedWriter> bufferedWriterByProjectId = null;
		Map<String, Path> filePathByProjectId = null;

		try {

			String root = AppConfigProperty.getAppConfigValue(Constant.AIMS_ROOT_URL);
			String extention = AppConfigProperty.getAppConfigValue(
					Constant.AIMS_MULTI_REGISTRATION_URI_KEY);
			String sourceSystemId = AppConfigProperty.getAppConfigValue(
					Constant.UDAS_SOURCE_SYSTEM_ID_KEY);

			if(root.isEmpty() || extention.isEmpty() || sourceSystemId.isEmpty()) {
				Logger.error("ArchiveRegistrationBatchJob AIMS Multiple Resgistration URL not " +
						"found in App Config");
				Logger.debug("ArchiveRegistrationBatchJob Stopping the iteration - " + 
						iterationId);
				return false;
			}

			processedCount = new AtomicInteger(0);

			String multiRegUrlProt = root + (extention.replace(
					Constant.AIMS_URL_SOURCE_SYS_ID_STRING, sourceSystemId));
			Logger.debug("ArchiveRegistrationBatchJob Multi Registration URL Prot: " + 
					multiRegUrlProt);

			RawSql rawSql = RawSqlBuilder.
					parse(Constant.INCOMPLETE_REGISTRATION_QUERY).
					columnMapping("m.GUID", "guid").
					columnMapping("m.PROJECT_ID", "projectId").
					columnMapping("m.FILE_NAME", "fileName").
					columnMapping("m.FILE_SIZE", "fileSize").
					columnMapping("m.RETENTION_START", "retentionStart").
					columnMapping("m.METADATA_SIZE", "metadataSize").
					columnMapping("m.INGESTION_END", "ingestionEnd").
					columnMapping("m.EVENT_BASED", "eventBased").
					columnMapping("mp.STATUS", "aimsRegistrationStatus").
					create();

			int CARS_BATCH_REGISTRATION_MAX_ROWS = Constant.CARS_BATCH_REGISTRATION_MAX_ROWS;
			AppConfigProperty appConfigProperty2 = AppConfigProperty.getPropertyByKey(
					Constant.CARS_BATCH_REGISTRATION_MAX_ROWS_KEY);

			if(appConfigProperty2 != null && appConfigProperty2.getIntValue() != null) {
				CARS_BATCH_REGISTRATION_MAX_ROWS = appConfigProperty2.getIntValue();
			}

			Logger.debug("ArchiveRegistrationBatchJob Batch Max Rows: " + 
					CARS_BATCH_REGISTRATION_MAX_ROWS);

			Query<Metadata> query = Ebean.find(Metadata.class).
					setUseQueryCache(false).
					select("guid").
					select("projectId").
					select("fileName").
					select("fileSize").
					select("retentionStart").
					select("metadataSize").
					select("ingestionEnd").
					select("eventBased").
					select("aimsRegistrationStatus").
					setMaxRows(CARS_BATCH_REGISTRATION_MAX_ROWS);


			query
			.setRawSql(rawSql)
			.where()
			.and(
					Expr.or(Expr.eq("m.STATUS", Constant.METADATA_STATUS_ARCHIVED), 
							Expr.eq("m.STATUS", Constant.METADATA_STATUS_USER_ERROR)),
					Expr.or(
							Expr.isNull("mp.STATUS"), 
							Expr.eq("mp.STATUS", 
									Integer.valueOf(
											Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_FAILED
											)
									)
							)
					);

			IncompleteRegistrationResultsetListener queryListener =
					new IncompleteRegistrationResultsetListener();
			query.setListener(queryListener);

			Logger.info("ArchiveRegistrationBatchJob Incomplete Reg. Query: " + 
					query.toString());

			// Results will be processed in Listener
			List<Metadata> EmptyList = query.findList();

			// Query processing completed by Listener
			bufferedWriterByProjectId =
					queryListener.getBufferedWriterByProjectId();

			filePathByProjectId =
					queryListener.getFilePathByProjectId();

			if(filePathByProjectId == null || filePathByProjectId.isEmpty()) {
				Logger.info("ArchiveRegistrationBatchJob Nothing to process.");
				Logger.debug("ArchiveRegistrationBatchJob Stopping the iteration - " + 
						iterationId);
				return false;
			}

			Logger.debug("ArchiveRegistrationBatchJob Project Map Count: " + 
					filePathByProjectId.size());
			
			Logger.debug("ArchiveRegistrationBatchJob To be Registered Count: " + 
					queryListener.getToBeRegisteredCount());

			final ExecutorService executorService = Executors.newFixedThreadPool(
					Runtime.getRuntime().availableProcessors());

			CyclicBarrier cyclicBarrier = new CyclicBarrier(
					filePathByProjectId.size(), new Runnable() {
						@Override
						public void run() {
							Logger.debug(
									"ArchiveRegistrationBatchJob Stopping the iteration - " + 
											iterationId);
							executorService.shutdown();
							synchronized (syncObject) {
								syncObject.notify();
							}
						}
					});

			for(Map.Entry<String, Path> entry : 
				filePathByProjectId.entrySet()) {
				String projectId = entry.getKey();
				Path filePath = entry.getValue();

				String multiRegUrl = 
						multiRegUrlProt.replace(
								Constant.AIMS_URL_PROJECT_ID_STRING, projectId);

				Logger.debug("ArchiveRegistrationBatchJob Adding task to executor for Project: " + 
						projectId + " File Path: " +  filePath + 
						" URL: " + multiRegUrl);

				executorService.execute(
						new RegistrationResponseProcessor(projectId, filePath, 
								multiRegUrl, cyclicBarrier));
			}
			return true;

		} catch (Exception e) {
			Logger.debug("ArchiveRegistrationBatchJob Stopping the iteration - " + 
					iterationId);
			Logger.error("ArchiveRegistrationBatchJob Stopping the iteration with exception - " + 
					iterationId, e);
			return false;
		} finally {
			try {
				if(bufferedWriterByProjectId != null) {
					for(Map.Entry<String, BufferedWriter> entry : 
						bufferedWriterByProjectId.entrySet()) {
						BufferedWriter bufferedWriter = entry.getValue();
						if(bufferedWriter != null) bufferedWriter.close();
					}
				}
			} catch (IOException e) {
				Logger.error("ArchiveRegistrationBatchJob - exception " +
						"occured while closing writers", e);
			}
		}
	}

	private class IncompleteRegistrationResultsetListener 
	implements QueryListener<Metadata> {


		private Map<String, BufferedWriter> bufferedWriterByProjectId = 
				new HashMap<String, BufferedWriter>();
		private Map<String, Path> filePathByProjectId = 
				new HashMap<String, Path>();
		
		private Integer toBeRegisteredCount = 0;

		private IncompleteRegistrationResultsetListener() {
		}

		public Map<String, BufferedWriter> getBufferedWriterByProjectId() {
			return bufferedWriterByProjectId;
		}

		public Map<String, Path> getFilePathByProjectId() {
			return filePathByProjectId;
		}
		
		public int getToBeRegisteredCount() {
			return toBeRegisteredCount;
		}

		@Override
		public void process(Metadata metadata) {

			try {
				Logger.debug("ArchiveRegistrationBatchJob QueryListener " + 
						" Metadata Guid: " +  metadata.getGuid() +
						" AIMS Reg Status: " + metadata.getAimsRegistrationStatus());
				if(metadata.getAimsRegistrationStatus() ==
						null || metadata.getAimsRegistrationStatus() == 
						Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_FAILED) {

					String projectId = metadata.getProjectId();

					if(projectId == null || projectId.trim().isEmpty()) {
						Logger.error("ArchiveRegistrationBatchJob Project Id null - Data Retroeval error.");
						return;
					}

					BufferedWriter bufferedWriter = 
							bufferedWriterByProjectId.get(projectId);

					if(bufferedWriter == null) {
						Path regsitrationJsonFile = Files.createTempFile("aimsreg_" + 
								projectId, System.currentTimeMillis() + "");

						bufferedWriter = 
								Files.newBufferedWriter(regsitrationJsonFile, 
										Charset.defaultCharset());

						Logger.debug("ArchiveRegistrationBatchJob QueryListener " + 
								" Metadata Guid: " +  metadata.getGuid() +
								" AIMS Reg Status: " + metadata.getAimsRegistrationStatus() +
								" Project: " + projectId + 
								" File Path: " + regsitrationJsonFile.toString());
						bufferedWriterByProjectId.put(projectId, bufferedWriter);
						filePathByProjectId.put(projectId,regsitrationJsonFile);
					}

					ArchiveFileRegistrationVO registrationVO =
							new ArchiveFileRegistrationVO();

					registrationVO.setLobArchiveFileId(metadata.getGuid());
					registrationVO.setProjectId(metadata.getProjectId());
					registrationVO.setArchiveSizeInBytes(metadata.getFileSize());
					Logger.debug("ArchiveRegistrationBatchJob - metadata retention date "+metadata.getRetentionStart());
					Logger.debug("ArchiveRegistrationBatchJob - reg retention date "+DateUtil.formatDateYYYY_MM_DD(
							new Date(metadata.getRetentionStart())));
					registrationVO.setRetentionStartDate(DateUtil.formatDateYYYY_MM_DD(
							new Date(metadata.getRetentionStart())));
					Logger.debug("ArchiveRegistrationBatchJob - registrationVO "+registrationVO.getRetentionStartDate());

					registrationVO.setIndexFileSize(metadata.getMetadataSize());
					registrationVO.setArchiveDescription(metadata.getFileName());
					registrationVO.setArchiveDate(DateUtil.formatDateYYYY_MM_DD(
							new Date(metadata.getIngestionEnd())));
					registrationVO.setEventBased(metadata.getEventBased());
					
					Logger.debug("ArchiveRegistrationBatchJob QueryListener " + 
							"ArchiveFileRegistrationVO: " + registrationVO);

					ObjectMapper objectMapper = new ObjectMapper();

					String jsonString = objectMapper.writeValueAsString(registrationVO);

					Logger.debug("ArchiveRegistrationBatchJob QueryListener " + 
							"ArchiveFileRegistrationVO JSON: " + jsonString);

					bufferedWriter.write(jsonString);
					bufferedWriter.write("\n");
					bufferedWriter.flush();
					
					toBeRegisteredCount++;
				}
			} catch (Exception e) {
				Logger.error("ArchiveRegistrationBatchJob Exception occurred while processing " +
						"IncompleteRegistrationResultsetListener", e);
			}
		}

	}

	private class RegistrationResponseProcessor implements Runnable {

		private final String projectId;
		private final Path filePath;
		private final String url;
		private final ObjectMapper objectMapper;
		private final CyclicBarrier cyclicBarrier;

		public RegistrationResponseProcessor(final String projectId, 
				final Path filePath, final String url, final CyclicBarrier cyclicBarrier) {
			this.projectId = projectId;
			this.filePath = filePath;
			this.url = url;
			this.cyclicBarrier = cyclicBarrier;
			objectMapper = new ObjectMapper();
		}

		@Override
		public void run() {
			InputStream inputStream = null;;
			try {
				Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
						"File Path: " + filePath);
				inputStream = Files.newInputStream(filePath);
				Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
						"Calling AIMS Service, " + 
						" URL: " + url +
						" File Path " + filePath);
				ChunkedInput<String> chunkedInput = 
						JerseyWSClient.postToAIMSAndGetChunkedResponse(
								url, inputStream, Constant.MULTI_REG_CHUNKED_PARSE_STRING);

				Logger.debug("RegistrationResponseProcessor " + 
						"Got  Chunked Input object from AIMS Service, " + 
						" ChunkedInput : " + chunkedInput);
				String chunk;

				while ((chunk = chunkedInput.read()) != null) {
					Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
							" Next chunk received: " + chunk);
					ArchiveFileRegistrationVO archiveFileRegistrationVO = null;
					try {
						archiveFileRegistrationVO = objectMapper.readValue(chunk, ArchiveFileRegistrationVO.class);
					} catch (Exception e) {
						Logger.error("ArchiveRegistrationBatchJob - JSON exception " +
								"occured while mapping ArchiveFileRegistrationVO", e);
					}
					if(archiveFileRegistrationVO == null) {
						continue;
					}
					if(archiveFileRegistrationVO.getAimsGuid() == null ||
							archiveFileRegistrationVO.getAimsGuid().trim().isEmpty() ||
							archiveFileRegistrationVO.getLobArchiveFileId() == null ||
							archiveFileRegistrationVO.getLobArchiveFileId().trim().isEmpty()) {
						Logger.warn("ArchiveRegistrationBatchJob RegistrationResponseProcessor " +
								"AIMSRegistration NOT saved: receievd bad response from AIMS: " + 
								chunk);
						continue;
					}
					Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
							" Received archiveFileRegistrationVO: " + archiveFileRegistrationVO);
					Metadata metadata = Metadata.findByGuid(
							archiveFileRegistrationVO.getLobArchiveFileId(), projectId);
					if(metadata == null) {
						Logger.error("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
								" Metadata Null for LobArchiveFileId: " + 
								archiveFileRegistrationVO.getLobArchiveFileId() +
								" Project Id: " + projectId);
						continue;
					}
					// check if the entry exists, update that.
					AIMSRegistration aimsRegistration = AIMSRegistration.findById(
							Helper.getMetadataId(metadata));
					if(aimsRegistration == null) {
						Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
								" AIMSRegistration Null for UDAS GUID: " + 
								archiveFileRegistrationVO.getLobArchiveFileId() +
								" Project Id: " + projectId +
								" Creaitng new instance." );
						aimsRegistration = AIMSRegistration.newInstance(
								archiveFileRegistrationVO, metadata);
					} else {
						Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
								" AIMSRegistration for UDAS GUID: " + 
								archiveFileRegistrationVO.getLobArchiveFileId() +
								" AIMS Guid: " + archiveFileRegistrationVO.getAimsGuid() +
								" Setting Reg status: " + 
								Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS);

						aimsRegistration.setAimsGuid(
								archiveFileRegistrationVO.getAimsGuid());
						aimsRegistration.setStatus(
								Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS);
					}
					Logger.debug("ArchiveRegistrationBatchJob RegistrationResponseProcessor " + 
							" Saving AIMSRegistration for UDAS GUID: " + 
							archiveFileRegistrationVO.getLobArchiveFileId() +
							" AIMS Guid: " + archiveFileRegistrationVO.getAimsGuid() +
							" Setting Reg status: " + 
							Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS);
					aimsRegistration.save();

					int currentCount = incrementProcessCount();
					if((currentCount % SLEEP_BATCH_COUNT) == 0) {
						try {
							Thread.sleep(SLEEP_MILLIS);
						} catch (Exception e) {
							Logger.error("ArchiveRegistrationBatchJob - RegistrationResponseProcessor exception " +
									"occured while Thread.sleep", e);
						}
					}
				}
			} catch(Exception e) {
				Logger.error("ArchiveRegistrationBatchJob - RegistrationResponseProcessor exception " +
						"occured while saving ArchiveFileRegistrationVO", e);
			} finally {
				try {
					if(inputStream != null) {
						inputStream.close();
					}
					if(filePath != null && !PropertyUtil.isDebug()) {
						Files.deleteIfExists(filePath);
					}
				} catch (Exception e) {
					Logger.error("ArchiveRegistrationBatchJob - RegistrationResponseProcessor exception " +
							"occured while deleting/ closing file streams", e);
				}
				try {
					if(cyclicBarrier != null) {
						cyclicBarrier.await();
					}
				} catch (InterruptedException e) {
					Logger.error("ArchiveRegistrationBatchJob - RegistrationResponseProcessor exception " +
							"occured while cyclicBarrier.await() InterruptedException", e);
				} catch (BrokenBarrierException e) {
					Logger.error("ArchiveRegistrationBatchJob - RegistrationResponseProcessor exception " +
							"occured while cyclicBarrier.await() BrokenBarrierException", e);
				}
			}
		}
	}

	public static String startTrigger() {		
		try
		{
			if (scheduler != null && trigger != null && trigger.getKey() != null && scheduler.getTriggerState(trigger.getKey()).equals(Trigger.TriggerState.NORMAL))
			{
				Logger.warn("ArchiveRegistrationBatchJob - scheduler is already running");
				return "ArchiveRegistrationBatchJob - "+Constant.SCHEDULER_ALREADY_RUNNING;
			}else{
				start();
				return Constant.SUCCESS;
			}
		}catch(Exception e)
		{
			Logger.error("ArchiveRegistrationBatchJob - Exception in rescheduling the job "+e);
			return "ArchiveRegistrationBatchJob "+e.getMessage();
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
					Logger.warn("ArchiveRegistrationBatchJob - Trigger/Key is null");
					return "ArchiveRegistrationBatchJob - "+Constant.TRIGGER_NOT_FOUND;
				}
			}
			else{
				Logger.warn("ArchiveRegistrationBatchJob - scheduler is null");
				return "ArchiveRegistrationBatchJob - "+Constant.SCHEDULER_NOT_FOUND;
			}
		}catch(Exception e)
		{
			Logger.error("ArchiveRegistrationBatchJob - Exception in unscheduling the job "+e);
			return "ArchiveRegistrationBatchJob "+e.getMessage();
		}
	}
	
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		try {
			if(!JobsUtil.isJobAllowedToRunOnThisHost("ArchiveRegistrationBatchJob"))
				return;
			final long iterationId = System.currentTimeMillis();
			final Object syncObject = new Object();
			Logger.info("ArchiveRegistrationBatchJob Starting iteration - " + 
					iterationId);
			synchronized (syncObject) {
				if(processIncompleteRegistrations(syncObject, iterationId)) {
					Logger.info("ArchiveRegistrationBatchJob Waiting on iteration - " + 
							iterationId);
					syncObject.wait();
				}
			}
			Logger.info("ArchiveRegistrationBatchJob Stopping iteration - " + 
					iterationId + ": " + getProcessedCount() + " pending archives registered.");
		} catch (Exception e) {
			Logger.error(e.getMessage() + e, e);
		}
	}
}
