package controllers;

import models.storageapp.RetentionCycle;
import models.storageapp.RCACycle;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import services.ValidateAnalysis;


public class AimsAdminConsoleRequestController extends Controller {

	public static Result validateRetentionScheduleAnalysis(){
		
		Logger.debug("AimsAdminConsoleRequestController - inside validateRetentionScheduleAnalysis ");
		String status = ValidateAnalysis.validateRetentionScheduleAnalysis();
		if(status.startsWith("An exception occurred"))
			return internalServerError(status);
		else
			return ok(status);
	}

	public static Result getLatestRetentionCycle() {
		return ok(Json.toJson(RetentionCycle.getCurrentCycleDetails()));
	}
	
	public static Result validateRecordCodeAlignmentAnalysis(){
		
		Logger.debug("AimsAdminConsoleRequestController - inside validateRecordCodeAlignmentAnalysis ");
		String status = ValidateAnalysis.validateRCAAnalysis();
		if(status.startsWith("An exception occurred"))
			return internalServerError(status);
		else
			return ok(status);
	}
	
	public static Result getLatestRCACycle() {
		return ok(Json.toJson(RCACycle.getCurrentCycleDetails()));
	}
	
	
}
