package playutils;

import play.Play;
import utilities.Constant;

public final class PropertyUtil {
	
	public static final int getAppLogLevel() {
		String logLevelString = 
				Play.application().configuration().getString("logger.application");
		
		if(logLevelString == null) return Constant.LOG_LEVEL_DEBUG;
		
		Integer logLvelInt = Constant.LOG_LEVEL_MAP.get(logLevelString);
		
		if(logLvelInt == null) return Constant.LOG_LEVEL_DEBUG;
		
		return logLvelInt;

	}
	
	public static final boolean isDebug() {
		return (getAppLogLevel() == Constant.LOG_LEVEL_DEBUG);
	}

}
