package utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateUtil {
	
/* 
 * DO NOT USE, NOT THREAD SAFE   
 * 
 * private final static SimpleDateFormat dateFormatYYYY_MM_DD = 
    		new SimpleDateFormat("yyyy-MM-dd");*/


	private DateUtil() {}

	public static long parseStringToTime(String formatString, String timeString) throws Exception {
		if (timeString == null || timeString.length() == 0) return 0L;
		boolean dateValidated = validateDateFormat(timeString);
		if(dateValidated){
			final SimpleDateFormat format = new SimpleDateFormat(formatString);
			format.setLenient(false);  //for strict date conversion; eg feb 29, 2011 or Sep 31, 2014, etc
			try {
				Date date = format.parse(timeString);
				if (date == null) return 0L;
				return date.getTime();
			} catch (ParseException e) {
				
			throw e;
			}
		}
		else{
			throw new Exception("Invalid Date. Please enter date in " + formatString + " format");
		}
	}

	public static String parseStringToOracleDate(String formatString, String timeString) throws Exception {
		if (timeString == null || timeString.length() == 0) return null;
		boolean dateValidated = validateDateFormat(timeString);
		if(dateValidated){
			final SimpleDateFormat format = new SimpleDateFormat(formatString);
			format.setLenient(false);  //for strict date conversion; eg feb 29, 2011 or Sep 31, 2014, etc
			try {
				Date date = format.parse(timeString);
				if (date == null) return null;
				return format.format(date);
			} catch (ParseException e) {
				
			throw e;
			}
		}
		else{
			throw new Exception("Invalid Date. Please enter date in " + formatString + " format");
		}
	}
	
	public static boolean validateDateFormat(String timeString){  
		   Matcher mtch = datePattern.matcher(timeString); 
		   if(!mtch.matches()){ return false;  }  

		   	int year = Integer.parseInt(mtch.group(1)); // range from 1- 5000
			int mm = Integer.parseInt(mtch.group(2)); // range from 1 - 12
			int dd =Integer.parseInt(mtch.group(3)); // range from 1 - 31

		   	if( (year < 1) || year>5000) {return false;	}
		   
			if(mm<1 || mm > 12) { return false;	}
			
			if(dd<1 || dd>31) {	return false;	}

		   return true;  
	}
	
    public static final String formatDateYYYY_MM_DD(java.util.Date date){
        if(date == null)
            return null;
        SimpleDateFormat dateFormatYYYY_MM_DD = 
        		new SimpleDateFormat("yyyy-MM-dd");
        return dateFormatYYYY_MM_DD.format(date);
    }
    
    public static final java.util.Date parseDateYYYY_MM_DD(
    		String dateStringYYYY_MM_DD){
    	if(dateStringYYYY_MM_DD != null){
    		java.util.Date date = null;
            try {
            	SimpleDateFormat dateFormatYYYY_MM_DD = 
                		new SimpleDateFormat("yyyy-MM-dd");
                date = dateFormatYYYY_MM_DD.parse(dateStringYYYY_MM_DD);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return date;
    	} else
    		return null;
    	
    }

    public static String getCurrentDate()
	{
		long yourmilliseconds = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");    
		Date resultdate = new Date(yourmilliseconds);
		return sdf.format(resultdate);
	}
	
    public static Date getDate()
   	{
   		Date date = new Date(System.currentTimeMillis());
   		return date;
   	}
	private final static Pattern datePattern = Pattern.compile("^([0-9]{4})-([0-9]{2})-([0-9]{2})$");
}
