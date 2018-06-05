package controllers;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;

import adapters.AdapterException;
import adapters.aims.AimsAdapter;
import adapters.aims.RecordCode.TimeUnit;
import adapters.aims.User;

public class AimsController extends Controller {
	
	private static AimsAdapter aimsAdapter = AimsAdapter.getInstance();

    private final static String remoteAddress = request().remoteAddress() + " - ";
	
    public static Result user(String username, String projectid ) {
        try {
            if (username == null || username.length() == 0) return badRequest();
            User user = aimsAdapter.getUserInfo(username);
            if (user == null) return notFound();
            return ok(Json.toJson(user));
        } catch (Exception e) {
            Logger.error(remoteAddress + e.getMessage());
            return internalServerError();
        }
    }

    public static Result project(String projectid) {
    	try {
    		return ok(Json.toJson(aimsAdapter.getProjectDetails(projectid)));
    	} catch (Exception e) {
            Logger.error(remoteAddress + e.getMessage());
    		return internalServerError();
    	}
    } 
    
    public static Result recordCode(String projectid) {
    	try {
    		return ok(Json.toJson(aimsAdapter.getRecordCode(projectid)));
    	} catch (Exception e) {
            Logger.error(remoteAddress + e.getMessage());
    		return internalServerError("Please pass the record code parameters as queryString for recordCode");
    	}
    } 

    public static Result rbac(String roleid, String username, String projectid) {
    	System.out.println("In Aims controller "+roleid+"  "+username+"   "+projectid);
        Boolean access = Boolean.FALSE;
        try {
            access = aimsAdapter.authorizeUser(username, roleid, projectid);
        } catch (AdapterException e) {
            Logger.error(remoteAddress + e.getMessage(), e);
            e.printStackTrace();
        }
        response().setHeader("x-rbac", access.toString());
        return ok(access.toString());
    }
    
    public static Result recordCode() {
    	try {
    		
    		String code=request().getQueryString("recordCode");
    		String countryCode=request().getQueryString("countryCode");
    		long retentionPeriod=Long.parseLong(request().getQueryString("retentionPeriod"));
    		String time=request().getQueryString("timeUnit");
    		String recordName=request().getQueryString("recordName");
    		TimeUnit timeUnit = TimeUnit.DAY;
    		
    		if(time.equalsIgnoreCase("Years") ){
    			timeUnit=TimeUnit.YEAR;
    			}
    		else if(time.equalsIgnoreCase("Months")){
    				timeUnit=TimeUnit.MONTH;
    			}
    		else if(time.equalsIgnoreCase("Days")){
    				timeUnit=TimeUnit.DAY;
    		}
    		return ok(Json.toJson(aimsAdapter.getRecordCode(code,countryCode,retentionPeriod,timeUnit,recordName,"")));
    	} catch (Exception e) {
            Logger.error(remoteAddress + e.getMessage());
    		return internalServerError();
    	}
    }	

    public static Result getDataSecurityDetails(String projectid) {
        try {
            return ok(aimsAdapter.getDataSecurityDetails(projectid));
        } catch (Exception e) {
            Logger.error(remoteAddress + e.getMessage());
            return internalServerError();
        }
    }

}