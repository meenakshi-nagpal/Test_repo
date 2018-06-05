package services;

import java.util.Date;

import jobs.aims.GetRecordCodeAlignmentDetailsJob;
import jobs.aims.GetRetentionUpdateBatchJob;
import jobs.util.UpdateProcessUtil;
import models.storageapp.RCACycle;
import models.storageapp.RetentionCycle;
import utilities.Constant;
import utilities.Utility;
import play.Logger;


public class ValidateAnalysis {

	
	public static String validateRetentionScheduleAnalysis() {
		try {
			RetentionCycle retentionCycle = RetentionCycle.getCurrentCycleDetails();
			Logger.info("Trying to validate the current cycle with id: "+retentionCycle.getCycleId());
			Logger.info(retentionCycle.toString());
			String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RETENTION_RUN_MODE);
			
			if (retentionCycle.getCurrentStatus() == Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED) {
				retentionCycle.setCurrentStatus(Constant.CYCLE_STATUS_ANALYSIS_VALIDATED);
				retentionCycle.setCycleDate(new Date());
				retentionCycle.update();
				Logger.info("The current cycle with id: "
						+ retentionCycle.getCycleId()
						+ " is validated successfully.");
				//Update the history table
				GetRetentionUpdateBatchJob.insertRetentionCycleHistory(retentionCycle,"",analysisValidationMode);
				return ("The current cycle with id: "
						+ retentionCycle.getCycleId()
						+ " is validated successfully.");
			} else if(retentionCycle.getCurrentStatus() == Constant.CYCLE_STATUS_ANALYSIS_VALIDATED) {
				Logger.info("Current cycle with id: " +  retentionCycle.getCycleId()+ ", is already validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(retentionCycle.getCurrentStatus())+" ("+retentionCycle.getCurrentStatus()+")");
				return ("Current cycle with id: " +  retentionCycle.getCycleId()+ ", is already validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(retentionCycle.getCurrentStatus())+" ("+retentionCycle.getCurrentStatus()+")");
			}else {
				Logger.info("Current cycle with  id:  " +  retentionCycle.getCycleId()+ ",  cannot be validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(retentionCycle.getCurrentStatus())+" ("+retentionCycle.getCurrentStatus()+")");
				return ("Current cycle with  id:  " +  retentionCycle.getCycleId()+ ",  cannot be validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(retentionCycle.getCurrentStatus())+" ("+retentionCycle.getCurrentStatus()+")");
			}
		} catch (Exception e) {
				Logger.error("An exception occurred while validating the retention cycle.",e);
			return ("An exception occurred while validating the retention cycle.");
		}

	}
	
	/**
	 * Validates the current RCA cycle with ANALYSIS_FULL_COMPLETED state
	 * to ANAYLYSIS_VALILDATED. This service is expected to be triggered by
	 * admin user.
	 * 
	 * @return
	 */
	public static String validateRCAAnalysis() {
		try {
			RCACycle rcaCycle = RCACycle.getCurrentCycleDetails();
			Logger.info("Trying to validate the current cycle with id: "+rcaCycle.getCycleId());
			Logger.info(rcaCycle.toString());
			String analysisValidationMode = UpdateProcessUtil.getProperty(Constant.APPLY_RCA_RUN_MODE);
			
			if (rcaCycle.getCurrentStatus() == Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED) {
				rcaCycle.setCurrentStatus(Constant.CYCLE_STATUS_ANALYSIS_VALIDATED);
				rcaCycle.setCycleDate(new Date());
				rcaCycle.update();
				Logger.info("The current cycle with id: "
						+ rcaCycle.getCycleId()
						+ " is validated successfully.");
				//Update the history table
				GetRecordCodeAlignmentDetailsJob.insertRCACycleHistory(rcaCycle,"",analysisValidationMode);
				return "The current cycle with id: "
						+ rcaCycle.getCycleId()
						+ " is validated successfully.";
			} else if(rcaCycle.getCurrentStatus() == Constant.CYCLE_STATUS_ANALYSIS_VALIDATED) {
				Logger.info("Current cycle with id: " +  rcaCycle.getCycleId()+ ", is already validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(rcaCycle.getCurrentStatus())+" ("+rcaCycle.getCurrentStatus()+")");
				return "Current cycle with id: " +  rcaCycle.getCycleId()+ ", is already validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(rcaCycle.getCurrentStatus())+" ("+rcaCycle.getCurrentStatus()+")";
			}else {
				Logger.info("Current cycle with  id:  " +  rcaCycle.getCycleId()+ ",  cannot be validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(rcaCycle.getCurrentStatus())+" ("+rcaCycle.getCurrentStatus()+")");
				return "Current cycle with  id:  " +  rcaCycle.getCycleId()+ ",  cannot be validated. Current status is: "
						+ Utility.getStatusMessageForRetentionStatuisId(rcaCycle.getCurrentStatus())+" ("+rcaCycle.getCurrentStatus()+")";
			}
		} catch (Exception e) {
				Logger.error("An exception occurred while validating the retention cycle.",e);
			return "An exception occurred while validating the retention cycle.";
		}

	}
	
	/*public static Result getLatestRCACycle() {
		return ok(Json.toJson(RCACycle.getCurrentCycleDetails()));
	}
	*/
	
}
