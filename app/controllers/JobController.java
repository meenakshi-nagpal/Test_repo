package controllers;

import java.util.Date;

import jobs.aims.ApplyRecordCodeAlignmentDetailsJob;
import jobs.aims.ApplyRetentionUpdatesJob;
import jobs.aims.ArchiveRegistrationBatchJob;
import jobs.aims.CandidatesFileIdMasterJob;
import jobs.aims.DeleteCandidateScheduler;
import jobs.aims.DestructionCandidatesJob;
import jobs.aims.UpdateRegistrationBatchJob;
import jobs.aims.IndexDetailsUpdationBatchJob;
import jobs.aims.LUDAIMSUpdateJob;
import jobs.aims.ProcessExpiredCandidatesScheduler;
import jobs.aims.GetRetentionUpdateBatchJob;
import jobs.aims.GetRecordCodeAlignmentDetailsJob;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;
import utilities.Constant;

public class JobController extends Controller {
	//TODO - logging...
	private static final String BATCH_JOB_NULL = "Batch Job is null";
	private static final String BATCH_JOB_NOT_FOUND = "Batch Job not found";
	private static final String INDEX_BATCH_JOB = "indexdetailsupdatebatchjob";
	private static final String CANDIDATE_FILEID_JOB = "candidatesfileidmasterjob";
	private static final String ARCHIVE_REG_JOB = "archiveregistrationbatchjob";
	private static final String DESTRUCT_CANDIDATE_JOB = "destructioncandidatesjob";
	private static final String DESTROY_CANDIDATE_JOB = "deletecandidatesjob";
	private static final String PROCESS_EXPIRED_CANDIDATE_JOB = "processexpiredcandidatesjob";
	private static final String LUD_UPDATE_JOB = "ludUpdateJob";
	private static final String UPDATE_REGISTRATION ="updateregistrationbatchjob";
	private static final String GET_RETENTION_UPDATE_JOB ="getretentionupdatesjob";
	private static final String APPLY_RETENTION_UPDATE ="applyretentionupdatesjob";
	private static final String GET_RECORD_CODE_ALIGNMENT_UPDATE_JOB ="getrecordcodealignmentdetailsjob";
	private static final String APPLY_RECORD_CODE_ALIGNMENT_UPDATE = "applyrecordcodealignmentdetailsjob";
	
	public static Result startAllJobs() {
		startIndexDetailsUpdationBatchJob();
		startCandidatesFileIdMasterJob();
		startArchiveRegistrationBatchJob();
		startDestructionCandidatesJob();
		startProcessExpiredCandidatesScheduler();
		startDeleteCandidateScheduler();
		startUDASLUDUpdateAIMSJob();
		startUpdateRegistrationBatchJob();
		startGetRetentionUpdateJob();
		startApplyRetentionUpdatesJob();
		//startApplyRecordCodeAlignmentDetailsJob();
		//startGetRecordCodeAlignmentDetailsJob();
		return ok("Triggered startAllJobs at " + new Date());
	}
	
	public static Result cancelAllJobs() {
		//TODO - add other jobs here
		cancelIndexDetailsUpdationBatchJob();
		cancelCandidatesFileIdMasterJob();
		cancelArchiveRegistrationBatchJob();
		cancelDestructionCandidatesJob();
		cancelProcessExpiredCandidatesScheduler();
		cancelDeleteCandidateScheduler();
		cancelUDASLUDUpdateAIMSJob();
		cancelUpdateRegistrationBatchJob();
		cancelGetRetentionUpdateJob();
		cancelApplyRetentionUpdatesJob();
		//cancelApplyRecordCodeAlignmentDetailsJob();
		//cancelGetRecordCodeAlignmentDetailsJob();
		return ok("Triggered cancelAllJobs at " + new Date());
	}
	
	public static Result cancelBatchJob(String batchjob)
	{
		Result result = null;
		if (batchjob == null)
		{
			return internalServerError(BATCH_JOB_NULL);
		}else
		{
			switch(batchjob.toLowerCase())
			{
			case INDEX_BATCH_JOB : result = cancelIndexDetailsUpdationBatchJob();break;
			case CANDIDATE_FILEID_JOB : result = cancelCandidatesFileIdMasterJob();break;
			case ARCHIVE_REG_JOB : result = cancelArchiveRegistrationBatchJob();break;
			case DESTRUCT_CANDIDATE_JOB : result = cancelDestructionCandidatesJob();break;
			case DESTROY_CANDIDATE_JOB : result = cancelDeleteCandidateScheduler();break;
			case PROCESS_EXPIRED_CANDIDATE_JOB : result = cancelProcessExpiredCandidatesScheduler();break;
			case LUD_UPDATE_JOB : result = cancelUDASLUDUpdateAIMSJob();break;
			case UPDATE_REGISTRATION : result = cancelUpdateRegistrationBatchJob();break;
			case GET_RETENTION_UPDATE_JOB : result = cancelGetRetentionUpdateJob();break;
			case APPLY_RETENTION_UPDATE : result = cancelApplyRetentionUpdatesJob();break;
			case GET_RECORD_CODE_ALIGNMENT_UPDATE_JOB : result = cancelGetRecordCodeAlignmentDetailsJob();break;
			case APPLY_RECORD_CODE_ALIGNMENT_UPDATE : result = cancelApplyRecordCodeAlignmentDetailsJob();break;
			default: result = internalServerError(BATCH_JOB_NOT_FOUND);
			}
			return result;
		}
	}
	
	public static Result runOnDemandBatchJob(String batchjob)
	{
		Result result = null;
		if (batchjob == null)
		{
			return internalServerError(BATCH_JOB_NULL);
		}else
		{
			switch(batchjob.toLowerCase())
			{
			case GET_RETENTION_UPDATE_JOB : result = runGetRetentionUpdateJob();break;
			case APPLY_RETENTION_UPDATE : result = runApplyRetentionUpdatesJob();break;
			case GET_RECORD_CODE_ALIGNMENT_UPDATE_JOB : result = runGetRecordCodeAlignmentDetailsJob();break;
			case APPLY_RECORD_CODE_ALIGNMENT_UPDATE : result = runApplyRecordCodeAlignmentDetailsJob();break;
			
			default: result = internalServerError(BATCH_JOB_NOT_FOUND);
			}
			return result;
		}
	}
	
	public static Result startIBatchJob(String batchjob)
	{
		Result result = null;
		if (batchjob == null)
		{
			return internalServerError(BATCH_JOB_NULL);
		}else
		{
			switch(batchjob.toLowerCase())
			{
			case INDEX_BATCH_JOB : result = startIndexDetailsUpdationBatchJob();break;
			case CANDIDATE_FILEID_JOB : result = startCandidatesFileIdMasterJob();break;
			case ARCHIVE_REG_JOB : result = startArchiveRegistrationBatchJob();break;
			case DESTRUCT_CANDIDATE_JOB : result = startDestructionCandidatesJob();break;
			case DESTROY_CANDIDATE_JOB : result = startDeleteCandidateScheduler();break;
			case PROCESS_EXPIRED_CANDIDATE_JOB : result = startProcessExpiredCandidatesScheduler();break;
			case LUD_UPDATE_JOB : result = startUDASLUDUpdateAIMSJob();break;
			case UPDATE_REGISTRATION : result = startUpdateRegistrationBatchJob();break;
			case GET_RETENTION_UPDATE_JOB : result = startGetRetentionUpdateJob();break;
			case APPLY_RETENTION_UPDATE : result = startApplyRetentionUpdatesJob();break;
			case GET_RECORD_CODE_ALIGNMENT_UPDATE_JOB : result = startGetRecordCodeAlignmentDetailsJob();break;
			case APPLY_RECORD_CODE_ALIGNMENT_UPDATE : result = startApplyRecordCodeAlignmentDetailsJob();break;
			
			default: result = internalServerError(BATCH_JOB_NOT_FOUND);
			}
			return result;
		}
	}
	
	public static Result cancelIndexDetailsUpdationBatchJob()
	{
		Logger.debug("JobController - inside cancelIndexDetailsUpdationBatchJob ");
		String status = IndexDetailsUpdationBatchJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("IndexDetailsUpdationBatchJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startIndexDetailsUpdationBatchJob()
	{
		Logger.debug("JobController - Triggering startIndexDetailsUpdationBatchJob ");
		
		String status = IndexDetailsUpdationBatchJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("IndexDetailsUpdationBatchJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelCandidatesFileIdMasterJob()
	{
		Logger.debug("JobController - inside cancelCandidatesFileIdMasterJob ");
		String status = CandidatesFileIdMasterJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("CandidatesFileIdMasterJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startCandidatesFileIdMasterJob()
	{
		Logger.debug("JobController - Triggering startCandidatesFileIdMasterJob ");
		
		String status = CandidatesFileIdMasterJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("CandidatesFileIdMasterJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelArchiveRegistrationBatchJob()
	{
		Logger.debug("JobController - inside cancelArchiveRegistrationBatchJob ");
		String status = ArchiveRegistrationBatchJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("ArchiveRegistrationBatchJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startArchiveRegistrationBatchJob()
	{
		Logger.debug("JobController - Triggering ArchiveRegistrationBatchJob ");
		
		String status = ArchiveRegistrationBatchJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("ArchiveRegistrationBatchJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelDestructionCandidatesJob()
	{
		Logger.debug("JobController - inside cancelDestructionCandidatesJob ");
		String status = DestructionCandidatesJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("cancelDestructionCandidatesJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startDestructionCandidatesJob()
	{
		Logger.debug("JobController - Triggering DestructionCandidatesJob ");
		
		String status = DestructionCandidatesJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("startDestructionCandidatesJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelProcessExpiredCandidatesScheduler()
	{
		Logger.debug("JobController - inside cancelProcessExpiredCandidatesScheduler ");
		String status = ProcessExpiredCandidatesScheduler.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("cancelProcessExpiredCandidatesScheduler stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startProcessExpiredCandidatesScheduler()
	{
		Logger.debug("JobController - Triggering cancelProcessExpiredCandidatesScheduler ");
		
		String status = ProcessExpiredCandidatesScheduler.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("startDestructionCandidatesJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelDeleteCandidateScheduler()
	{
		Logger.debug("JobController - inside cancelDeleteCandidateScheduler ");
		String status = DeleteCandidateScheduler.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("cancelDeleteCandidateScheduler stopped at "+new Date());
		else
			return internalServerError(status);
	}

	public static Result cancelUDASLUDUpdateAIMSJob()
	{
		Logger.debug("JobController - inside UDASLUDUpdateAIMSJob ");
		String status = LUDAIMSUpdateJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("UDASLUDUpdateAIMSJob stopped at "+new Date());
		else
			return internalServerError(status);
	}

	public static Result startDeleteCandidateScheduler()
	{
		Logger.debug("JobController - Triggering startDeleteCandidateScheduler ");
		
		String status = DeleteCandidateScheduler.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("startDeleteCandidateScheduler started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result startUDASLUDUpdateAIMSJob() {
		Logger.debug("JobController - Triggering startUDASLUDUpdateAIMSJob ");
		
		String status = LUDAIMSUpdateJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("startUDASLUDUpdateAIMSJob started at "+new Date());
		else
			return internalServerError(status);
	}

	public static Result cancelUpdateRegistrationBatchJob()
	{
		Logger.debug("JobController - inside cancelUpdateRegistrationBatchJob ");
		String status = UpdateRegistrationBatchJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("UpdateRegistrationBatchJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	
	public static Result startUpdateRegistrationBatchJob()
	{
		Logger.debug("JobController - Triggering UpdateRegistrationBatchJob ");
		
		String status = UpdateRegistrationBatchJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("UpdateRegistrationBatchJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelGetRetentionUpdateJob()
	{
		Logger.debug("JobController - inside cancelGetRetentionUpdateJob ");
		String status = GetRetentionUpdateBatchJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("GetRetentionUpdateJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	
	public static Result startGetRetentionUpdateJob()
	{
		Logger.debug("JobController - Triggering startGetRetentionUpdateJob ");
		
		String status = GetRetentionUpdateBatchJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("GetRetentionUpdateJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result cancelApplyRetentionUpdatesJob()
	{
		Logger.debug("JobController - inside cancelApplyRetentionUpdatesJob ");
		String status = ApplyRetentionUpdatesJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("ApplyRetentionUpdatesJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	
	public static Result startApplyRetentionUpdatesJob()
	{
		Logger.debug("JobController - Triggering ApplyRetentionUpdatesJob ");
		
		String status = ApplyRetentionUpdatesJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("ApplyRetentionUpdatesJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result runApplyRetentionUpdatesJob()
	{
		Logger.debug("JobController - Triggering ApplyRetentionUpdatesJob ");
		
		String aitParam = request().getQueryString("ait");
		String projectIdParam = request().getQueryString("projectid");
		String recordCodeParam = request().getQueryString("recordcode");
		String modeParam = request().getQueryString("mode");
		String status = ApplyRetentionUpdatesJob.runOnDemand(aitParam,projectIdParam,recordCodeParam,modeParam);
		
		if (status.equals(Constant.SUCCESS)){ 
			return ok("ApplyRetentionUpdatesJob ran successfully at "+new Date());
		}else if (status.startsWith(Constant.SUCCESS_WITH_WARNING)){
			String[] st = status.split("-");
			return ok(st[1]);
		}else{
			return internalServerError(status);
		}
	}
	
	
	public static Result runGetRetentionUpdateJob()
	{
		Logger.debug("JobController - Triggering GetRetentionUpdateBatchJob ");
		
	    String status = GetRetentionUpdateBatchJob.runOnDemand();
	    if (status.equals(Constant.SUCCESS)) 
			return ok("GetRetentionUpdateBatchJob started at "+new Date());
		else if (status.startsWith(Constant.SUCCESS_WITH_WARNING)){
			String[] st = status.split(",");
			return ok(st[1]);
		}else{
			return internalServerError(status);
		}
	}
	
	public static Result cancelApplyRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - inside ApplyRecordCodeAlignmentDetailsJob ");
		String status = ApplyRecordCodeAlignmentDetailsJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("ApplyRecordCodeAlignmentDetailsJob stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	
	public static Result startApplyRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - Triggering ApplyRecordCodeAlignmentDetailsJob ");
		
		String status = ApplyRecordCodeAlignmentDetailsJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("ApplyRecordCodeAlignmentDetailsJob started at "+new Date());
		else
			return internalServerError(status);
	}
	
	public static Result runApplyRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - Triggering ApplyRecordCodeAlignmentDetailsJob ");
		
		String aitParam = request().getQueryString("ait");
		String projectIdParam = request().getQueryString("projectid");
		String modeParam = request().getQueryString("mode");
		String status = ApplyRecordCodeAlignmentDetailsJob.runOnDemand(aitParam,projectIdParam,modeParam);
		
		if (status.equals(Constant.SUCCESS)){ 
			return ok("ApplyRecordCodeAlignmentDetailsJob ran successfully at "+new Date());
		}else if (status.startsWith(Constant.SUCCESS_WITH_WARNING)){
			String[] st = status.split("-");
			return ok(st[1]);
		}else{
			return internalServerError(status);
		}
	}
	
	
	public static Result cancelGetRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - inside cancelGetRecordCodeAlignmentDetailsJob ");
		String status = GetRecordCodeAlignmentDetailsJob.stopTrigger();
		if (status.equals(Constant.SUCCESS)) 
			return ok("GetRecordCodeAlignmentDetailsJobs stopped at "+new Date());
		else
			return internalServerError(status);
	}
	
	
	public static Result startGetRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - Triggering startGetRecordCodeAlignmentDetailsJob ");
		
		String status = GetRecordCodeAlignmentDetailsJob.startTrigger();
		
		if (status.equals(Constant.SUCCESS)) 
			return ok("GetRecordCodeAlignmentDetailsJob started at "+new Date());
		else
			return internalServerError(status);
	}

	
	public static Result runGetRecordCodeAlignmentDetailsJob()
	{
		Logger.debug("JobController - Triggering GetRecordCodeAlignmentDetailsJob ");
		
	    String status = GetRecordCodeAlignmentDetailsJob.runOnDemand();
	    if (status.equals(Constant.SUCCESS)) 
			return ok("GetRecordCodeAlignmentDetailsJob started at "+new Date());
		else if (status.startsWith(Constant.SUCCESS_WITH_WARNING)){
			String[] st = status.split(",");
			return ok(st[1]);
		}else{
			return internalServerError(status);
		}
	}
	
}
