package jobs.util;

import java.net.InetAddress;

import play.Logger;
import play.Play;

import models.storageapp.AppConfigProperty;
import utilities.Constant;

public class JobsUtil {
	
	public static boolean isJobAllowedToRunOnThisHost(String jobName) {
		
		try {
			if(Play.isDev()) return true;
			String hostname = InetAddress.getLocalHost().getHostName();
			Logger.debug("UDAS SERVER HOSTNAME: " + hostname);
			
			String jobHost = AppConfigProperty.getAppConfigValue(
					Constant.JOBS_EXECUTION_HOST_KEY);
			
			if(hostname == null || hostname .isEmpty()) return true;
			
			if(jobHost == null || jobHost.isEmpty()) return true;
			
			if(jobHost.equals(hostname)) return true;
			
			Logger.info(jobName + " is not allowed to run on this host.");
			
			return false;
		} catch (Exception e) {
			Logger.error("JobsUtil - exception occurred while " +
					"Getting Jobs Host Name", e);
			e.printStackTrace();
		}
		
		return true;
	}

}
