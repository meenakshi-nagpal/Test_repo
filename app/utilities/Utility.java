package utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Utility {

	public static String getFormattedDate(Date date) {
		return new SimpleDateFormat(Constant.DATE_FORMAT).format(date);
	}

	public static String addPeriod(Date date, int period) {
		SimpleDateFormat parser = new SimpleDateFormat(Constant.DATE_FORMAT);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.SECOND, period);
		return parser.format(cal.getTime());
	}

	public static long getCurrentTime() {
		return System.currentTimeMillis();
	}

	public static Map<String, String> getMessage(String success, String action,
			String message) {
		Map<String, String> result = new HashMap<String, String>();
		result.put("success", success);
		result.put("action", action);
		result.put("Message", message);
		return result;
	}

	public static String join(List<String> strList, String joiner) {
		StringBuilder str = new StringBuilder(" ");
		if (strList != null && strList.size() > 0) {

			int i = 0;
			for (i = 0; i < strList.size() - 1; i++) {
				str.append(strList.get(i) + " " + joiner);

			}
			str.append(strList.get(i));
		}
		return str.toString();

	}

	public static boolean isNullOrEmpty(String str) {
		if (str == null || str.length() < 1)
			return true;
		else
			return false;
	}

	public static int getCuurentMonth() {
		return Calendar.getInstance().get(Calendar.MONTH) + 1; // counter starts
		// from 0 as Jan
	}

	public static int getCuurentYear() {
		return Calendar.getInstance().get(Calendar.YEAR);
	}

	public static int getCuurentDay() {
		return Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
	}

	public static String getFileExtention(String data) {
		if (data.trim().startsWith("<xml"))
			return ".xml";
		else if (data.trim().startsWith("{"))
			return ".json";
		else
			return ".txt";
	}

	public static Date getSafeExpiration() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE,
				Constant.DAYS_ALLOWED_TO_DELETE_BEFORE_EXPIRATION);
		return cal.getTime();
	}

	public static <T> JsonNode writeListToJsonArray(List<T> list) {

		JsonNode jsonNode = null;
		final OutputStream out = new ByteArrayOutputStream();
		final ObjectMapper mapper = new ObjectMapper();

		try {
			mapper.writeValue(out, list);
			final byte[] data = ((ByteArrayOutputStream) out).toByteArray();
			jsonNode = mapper.readTree(data);
			return jsonNode;
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return jsonNode;
	}

	public static TimeUnit getTimeUnit(int unit) {

		switch(unit)
		{
		case 0: return TimeUnit.MILLISECONDS;
		case 1: return TimeUnit.SECONDS;
		case 2: return TimeUnit.MINUTES;
		case 3: return TimeUnit.HOURS;
		case 4: return TimeUnit.DAYS;
		default: return TimeUnit.HOURS;
		}
	}

	public static String getAbsolutePEAFilePath(String fileName) {
		String peaRootDir = System.getProperty(Constant.PEA_ROOT_DIR);
		Path path = FileSystems.getDefault().getPath(peaRootDir, fileName);

		if(!Files.exists(path)) {
			throw new IllegalStateException("PEA file does not exist - " + 
					path.toAbsolutePath().toString());
		}

		return path.toAbsolutePath().toString();
	}
	
	public static String getCASPoolInfoString(String nodeIPs, String peaAbsolutePath) {
		return (nodeIPs + Constant.CAS_POOL_STRING_SEPARATOR + peaAbsolutePath);
	}

	
	/**
	 * Gives the proper string status for the given status integer
	 * @param statusId
	 * @return
	 */
	public static String getStatusMessageForRetentionStatuisId(int statusId) {

		String statusMsg = null;
		switch (statusId) {
		case Constant.CYCLE_STATUS_CYCLE_STARTED:

			statusMsg = "RETENTION_CYCLE_STATUS_CYCLE_STARTED";
			break;
		case Constant.CYCLE_STATUS_ANALYSIS_INPROGRESS:
			statusMsg = "RETENTION_CYCLE_STATUS_ANALYSIS_INPROGRESS";
			break;

		case Constant.CYCLE_STATUS_ANALYSIS_PARTIAL_COMPLETED:
			statusMsg = "RETENTION_CYCLE_STATUS_ANALYSIS_PARTIAL_COMPLETED";
			break;

		case Constant.CYCLE_STATUS_ANALYSIS_FULL_COMPLETED:
			statusMsg = "RETENTION_CYCLE_STATUS_ANALYSIS_FULL_COMPLETED";
			break;

		case Constant.CYCLE_STATUS_ANALYSIS_VALIDATED:
			statusMsg = "RETENTION_CYCLE_STATUS_ANALYSIS_VALIDATED";
			break;

		case Constant.CYCLE_STATUS_UPDATES_INPROGRESS:
			statusMsg = "RETENTION_CYCLE_STATUS_UPDATES_INPROGRESS";
			break;

		case Constant.CYCLE_STATUS_UPDATES_PARTIAL_COMPLETED:
			statusMsg = "RETENTION_CYCLE_STATUS_UPDATES_PARTIAL_COMPLETED";
			break;

		case Constant.CYCLE_STATUS_UPDATES_FULL_COMPLETED:
			statusMsg = "RETENTION_CYCLE_STATUS_UPDATES_FULL_COMPLETED";
			break;

		case Constant.CYCLE_STATUS_UPDATES_CONFIRMED:
			statusMsg = "RETENTION_CYCLE_STATUS_UPDATES_CONFIRMED";
			break;

		case Constant.CYCLE_STATUS_CYCLE_COMPLETED:
			statusMsg = "RETENTION_CYCLE_STATUS_CYCLE_COMPLETED";
			break;

		default:
			statusMsg = "Invalid Status";
			break;
		}

		return statusMsg;
	}
	

	public static long getRetentionEndInSeconds(Long retentionPeriod, Long retentionEndDate) {
		if(retentionPeriod==-1L){ 
			return -1L;
		}else{
			return retentionEndDate / 1000L;	
		}
	}	
	
	
}