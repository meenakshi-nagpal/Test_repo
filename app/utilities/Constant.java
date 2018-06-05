package utilities;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Constant {

	public static final String DATE_FORMAT = "yyyy.MM.dd HH:mm:ss z";
	public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss z";
	public static final String MATURITY_DATE_FORMAT = "yyyy-MM-dd";
	public static final long EXTENDED_METADATA_SIZE_LIMIT =  1073741824L;	//in bytes
	public static final String EXTENDED_METADATA_CONTAINER_DIR = "archive_metadata";
	public static final String 	RBAC_HEADER_KEY		=  "x-rbac";
	public static final long PLAY_MAX_SIZE = 2147483647L;
	
	//Cron job expressions
	public static final String DESTRUCTION_CANDIDATES_BATCH_JOB_CRON_EXPR="DESTRUCTION_CANDIDATES_BATCH_JOB_CRON_EXPR";
	public static final String DELETE_CANDIDATE_BATCH_JOB_CRON_EXPR="DELETE_CANDIDATE_BATCH_JOB_CRON_EXPR";
	public static final String CANDIDATE_FILE_BATCH_JOB_CRON_EXPR="CANDIDATE_FILE_BATCH_JOB_CRON_EXPR";
	public static final String INDEX_UPDATE_BATCH_JOB_CRON_EXPR="INDEX_UPDATE_BATCH_JOB_CRON_EXPR";
	public static final String ARCHIVE_REGISTRATION_BATCH_JOB_CRON_EXPR="ARCHIVE_REGISTRATION_BATCH_JOB_CRON_EXPR";
	public static final String PROCESS_EXPIRED_BATCH_JOB_CRON_EXPR="PROCESS_EXPIRED_BATCH_JOB_CRON_EXPR";
	public static final String EXTEND_RETENTION_FOR_EBR_FILES_JOB_CRON_EXPR = "EXTEND_RETENTION_FOR_EBR_FILES_JOB_CRON_EXPR";
	public static final String REGISTRATION_UPDATE_BATCH_JOB_CRON_EXPR="REGISTRATION_UPDATE_BATCH_JOB_CRON_EXPR";
	public static final String GET_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR="GET_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR";
	public static final String APPLY_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR="APPLY_RETENTION_UPDATE_BATCH_JOB_CRON_EXPR";
	public static final String GET_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR="GET_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR";
	public static final String APPLY_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR="APPLY_RECORD_CODE_ALIGNMENT_BATCH_JOB_CRON_EXPR";
	
	public static final String AIMS_REGISTRATION_STATUS_KEY="status";
	public static final int AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS = 1;
	public static final int AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_FAILED = 2;
	public static final int AIMS_REGISTRATION_STATUS_INDEX_REGISTRATION_FAILED = 3;
	public static final int AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE = 4;
	public static final int AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_PENDING = 5;
	public static final int AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_FAILED = 6;
	public static final int AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_SUCCESS = 7;
	
	public static final String AIMS_URL_PROJECT_ID_STRING = "{projectId}";
	public static final String AIMS_URL_SOURCE_SYS_ID_STRING = "{sourceSystem}";
	public static final String AIMS_URL_AIMS_ID_STRING = "{aimsId}";
	
	public static final int CANDIDATES_PROCESS_STATUS_READY = 1;
	public static final int CANDIDATES_PROCESS_STATUS_RECEIVED = 2;
	public static final int CANDIDATES_PROCESS_STATUS_PENDING = 3;
	public static final int CANDIDATES_PROCESS_STATUS_LOCKED = 4;
	public static final int CANDIDATES_PROCESS_STATUS_PROCESSED = 5;
	
	public static final int DAYS_ALLOWED_TO_DELETE_BEFORE_EXPIRATION = 5;
	
	public static final String AIMS_CANDIDATE_FILE_ID_MASTER_URL_KEY = 
			"AIMS_CANDIDATE_FILE_ID_MASTER_URL";
	public static final String UDAS_SOURCE_SYSTEM_ID_KEY = "UDAS_SOURCE_SYSTEM_ID";
	
	public static final String AIMS_DESTRUCTION_LIST_URL_KEY = 
			"AIMS_DESTRUCTION_LIST_URL";
	
	public static final int DEFAULT_AIMS_DESTRUCTION_LIST_JOB_FREQUENCY_HOURS = 24;
	public static final int DEFAULT_AIMS_ARCHIVE_REGISTRATION_BATCH_JOB_FREQUENCY_HOURS = 6;
	public static final String AIMS_ARCHIVE_REGISTRATION_BATCH_JOB_FREQUENCY_HOURS_KEY = 
			"AIMS_ARCHIVE_REGISTRATION_BATCH_JOB_FREQUENCY_HOURS";
	
	public static final String AIMS_ROOT_URL = "AIMS_ROOT_URL";
	public static final String DELETE_CONFIRMATION_SERVICE_URL = "DELETE_CONF_SERVICE_URL";
	
	public static final String INCOMPLETE_REGISTRATION_QUERY = "SELECT m.GUID, " +
			"  m.PROJECT_ID, " +
			"  m.FILE_NAME, " +
			"  m.FILE_SIZE, " +
			"  m.RETENTION_START, " +
			"  m.METADATA_SIZE, " +
			"  m.INGESTION_END, " +
			"  m.EVENT_BASED, " +
			"  mp.STATUS " +
			"FROM ILM_METADATA m " +
			"LEFT JOIN AIMS_REGISTRATION mp " +
			"ON (m.ID          = mp.METADATA_ID) ";// +
/*			"WHERE (mp.STATUS IS NULL " +
			"OR mp.STATUS     = " + 
			Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_FAILED + ")";*/
	
	public static final String AIMS_WS_TIMEOUT_SECONDS_KEY = "AIMS_WS_TIMEOUT_SECONDS";
	public static final int DEFAULT_AIMS_WS_TIMEOUT_SECONDS = 5;
	
	public static final String AIMS_MULTI_REGISTRATION_URI_KEY = 
			"AIMS_MULTI_REGISTRATION_URI";

	//RIMS ERROR Code and Value
	public static final String ERROR_CODE_A = "Record was not found for Deletion";
	public static final String ERROR_CODE_B = "Record Unique Identifier is invalid";
	public static final String ERROR_CODE_C = "System Failure";
	public static final String ERROR_CODE_D = "Record destruction could not be processed in the allowed time window";
	public static final String ERROR_CODE_E = "Record Identified is locked and cannot be deleted at this time";
	public static final String ERROR_CODE_F = "Record is being held for preservation or analysis by system";
	public static final String ERROR_CODE_G = "Original data provided is in error and therefore disposition is invalid";
	public static final String ERROR_CODE_H = "Destruction Request Withdrawn";
	public static final String ERROR_CODE_I = "General Error - Unknown";
	public static final String ERROR_CODE_J = "Disposition File is not understood / invalid";
	public static final String ERROR_CODE_K = "Media Corrupted";
	
	public static final String AIMS_SINGLE_REGISTRATION_URI_KEY = 
			"AIMS_SINGLE_REGISTRATION_URI";
	
	public static final String AIMS_USER = "aims.user";
	public static final String AIMS_PASSWORD = "aims.password";
	public static final String AIMS_WS_TIMEOUT_SECONDS = "aims.ws.timeout.seconds";
	
	public static final int CARS_BATCH_REGISTRATION_MAX_ROWS = 1000;
	public static final String CARS_BATCH_REGISTRATION_MAX_ROWS_KEY = 
			"CARS_BATCH_REGISTRATION_MAX_ROWS";
	
	public static final int LOG_LEVEL_INFO = 1;
	public static final int LOG_LEVEL_ERROR = 2;
	public static final int LOG_LEVEL_DEBUG = 3;
	
	public static final Map<String, Integer>  LOG_LEVEL_MAP = 
			new HashMap<String, Integer>();
	static {
		LOG_LEVEL_MAP.put("INFO", 1);
		LOG_LEVEL_MAP.put("ERROR", 1);
		LOG_LEVEL_MAP.put("DEBUG", 1);
		LOG_LEVEL_MAP.put("info", 1);
		LOG_LEVEL_MAP.put("error", 1);
		LOG_LEVEL_MAP.put("debug", 1);
	}
	
	public static final String MULTI_REG_CHUNKED_PARSE_STRING = "\r\n";
	
	public static final String ARCHIVE_REGISTRATION_BATCH_JOB_INITIAL_DELAY_KEY = 
			"ARCHIVE_REGISTRATION_BATCH_JOB_INITIAL_DELAY";
	public static final String ARCHIVE_REGISTRATION_BATCH_JOB_FREQUENCY_KEY = 
			"ARCHIVE_REGISTRATION_BATCH_JOB_FREQUENCY";
	
	public static final Map<String, TimeUnit> TIME_UNIT_MAP = 
			new HashMap<String, TimeUnit>();
	static {
		TIME_UNIT_MAP.put("SECONDS", TimeUnit.SECONDS);
		TIME_UNIT_MAP.put("MINUTES", TimeUnit.MINUTES);
		TIME_UNIT_MAP.put("HOURS", TimeUnit.HOURS);
		TIME_UNIT_MAP.put("DAYS", TimeUnit.DAYS);
	}
	
	public static final String METADATA_STATUS_ARCHIVED = "ARCHIVED";
	public static final String METADATA_STATUS_USER_ERROR = "USER_ERROR";
	public static final String METADATA_HISTORY_STATUS_DELETED = "DELETED";
	public static final String METADATA_HISTORY_STATUS_FAILED = "FAILED";
	//Added these for updating index file size details for AIMS ARCHIVE_FILE_MASTER
	public static final String AIMS_URL_GUID_STRING = "{aimsGuid}";
	public static final String AIMS_REGISTRATION_UPDATE_INDEX_URL_KEY="AIMS_REGISTRATION_UPDATE_INDEX_URL";
	public static final String AIMS_REGISTRATION_UPDATE_EVENT_BASED_URL_KEY="AIMS_REGISTRATION_UPDATE_EVENT_BASED_URL";
	public static final String JOBS_EXECUTION_HOST_KEY = "JOBS_EXECUTION_HOST";
	
	public static final String DESTRUCTION_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_KEY ="DESTRUCTION_CANDIDATE_JOB_RECORD_PROCESS_LIMIT";
	public static final int DESTRUCTION_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_DEFAULT = 2000;
	public static final String DELETE_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_KEY ="DELETE_CANDIDATE_JOB_RECORD_PROCESS_LIMIT";
	public static final int DELETE_CANDIDATE_JOB_RECORD_PROCESS_LIMIT_DEFAULT = 2000;
	public static final String AIMS_DISPOSITION_JOB_CYCLE = "AIMS_DISPOSITION_JOB_CYCLE";
	
	public static final String SUCCESS="SUCCESS";
	public static final String SUCCESS_WITH_WARNING="SUCCESS WITH WARNING";
	public static final String SCHEDULER_NOT_FOUND="Scheduler not found";
	public static final String TRIGGER_NOT_FOUND="Trigger not found";
	public static final String DEFENSIBLE_DISPOSITION_JOBS="Defensible Disposition Jobs";
	public static final String RETENTION_UPDATE_JOBS="Retention Update Jobs";
	public static final String SCHEDULER_ALREADY_RUNNING="Scheduler already running";
	public static final String RESCHEDULED_JOB="Batch job is rescheduled";
	
	public static final String CURRENT_WRITE_STORAGE_KEY = "CURRENT_WRITE_STORAGE";
	public static final String CURRENT_WRITE_STORAGE_POOL_INFO_KEY = 
			"CURRENT_WRITE_STORAGE_POOL_INFO";
	
	public static final String CAS_POOL_STRING_SEPARATOR = "?";
	
	public static final int STORAGE_FAILOVER_STATUS = 6;
	
	public static final String PEA_ROOT_DIR = "pea.root.dir";
	
	public static final String CURRENT_WRITE_STORAGE_APP_PROPERTY = "current.write.storage";
	
	
	public static final String LUD_UPDATE_BATCH_JOB_CRON_EXPR="LUD_UPDATE_BATCH_JOB_CRON_EXPR";
	public static final String LUD_JOB_LAST_RUN_DATE = "LUD_JOB_LAST_RUN_DATE";

	public static final int CYCLE_STATUS_CYCLE_NOT_STARTED = 0;
	public static final int CYCLE_STATUS_CYCLE_STARTED = 1;
	public static final int CYCLE_STATUS_ANALYSIS_INPROGRESS = 2;
	public static final int CYCLE_STATUS_ANALYSIS_PARTIAL_COMPLETED = 3;
	public static final int CYCLE_STATUS_ANALYSIS_FULL_COMPLETED = 4;
	public static final int CYCLE_STATUS_ANALYSIS_VALIDATED = 5;
	public static final int CYCLE_STATUS_UPDATES_INPROGRESS = 6;
	public static final int CYCLE_STATUS_UPDATES_PARTIAL_COMPLETED = 7;
	public static final int CYCLE_STATUS_UPDATES_FULL_COMPLETED = 8;
	public static final int CYCLE_STATUS_UPDATES_CONFIRMED = 9;
	public static final int CYCLE_STATUS_CYCLE_COMPLETED = 10;
	
	public static final String AIMS_RETENTION_UPDATE_LIST_URL = "AIMS_RETENTION_UPDATE_LIST_URL";
	public static final String AIMS_RETENTION_ALERT_NOTIFIED_URL = "AIMS_RETENTION_ALERT_NOTIFIED_URL";
	public static final String APPLY_RETENTION_BATCH_COUNT = "APPLY_RETENTION_BATCH_COUNT";
	public static final String AIMS_RETENTION_UPDATE_CONFIRMATION_URL = "AIMS_RETENTION_UPDATE_CONFIRMATION_URL";
	public static final String APPLY_RETENTION_MAX_RUN_HOURS = "APPLY_RETENTION_MAX_RUN_HOURS";
	
	public static final String APPLY_RETENTION_EMAIL_TO = "APPLY_RETENTION_EMAIL_TO";
	public static final String APPLY_RETENTION_EMAIL_FROM = "APPLY_RETENTION_EMAIL_FROM";
	public static final String APPLY_RETENTION_EMAIL_SUBJECT = "APPLY_RETENTION_EMAIL_SUBJECT";
	public static final String APPLY_RETENTION_EMAIL_REPORT_LINK = "APPLY_RETENTION_EMAIL_REPORT_LINK";
	public static final String APPLY_RETENTION_EMAIL_VALIDATE_LINK = "APPLY_RETENTION_EMAIL_VALIDATE_LINK";
	public static final String APPLY_RETENTION_RUN_MODE = "APPLY_RETENTION_RUN_MODE";
	public static final String MANUAL = "MANUAL";
	public static final String AUTO = "AUTO";
	public static final String SMTP_SERVER = "SMTP_SERVER";
	
	public static final String AIMS_RECORD_CODE_ALIGNMENT_DETAILS_LIST_URL = "AIMS_RECORD_CODE_ALIGNMENT_DETAILS_LIST_URL";
	public static final String AIMS_RECORD_CODE_ALIGNMENT_NOTIFIED_URL = "AIMS_RECORD_CODE_ALIGNMENT_NOTIFIED_URL";
	
	public static final String trueValue ="Y";
	public static final String falseValue ="N";
	
	public static final String UNTRIGGERED = "EBR_UNTRIGGERED";
	public static final String DISABLED = "EBR_DISABLED";
	public static final String PERMOFF = "PERM-OFF";
	public static final String EBR_TO_EBR_EXT = "EBR_TO_EBR_EXT";
	public static final String EBR_TO_EBR_SHO = "EBR_TO_EBR_SHO";
	public static final String EBR_TO_EBR_NO_CHANGE = "EBR_TO_EBR_NO_CHANGE";
	public static final String TRIGGERED = "TRIGGERED";
	public static final String ENABLED = "ENABLED";

	public static final Long MAX_DATE = 253399640400000L;
	
	//HCP and S3 Active Configuration
	public static final String CURRENT_WRITE_STORAGE_TYPE = "CURRENT_WRITE_STORAGE_TYPE";
	
	public static final String HCP_CURRENT_WRITE_STORAGE_ID ="HCP_CURRENT_WRITE_STORAGE_ID";
	public static final String HCP_CURRENT_REST_END_URL = "HCP_REST_END_URL";
	public static final String HCP_CURRENT_S3_END_URL = "HCP_S3_END_URL";
	public static final String HCP_CURRENT_BUCKET = "HCP_BUCKET";
	public static final String HCP_CURRENT_ACCESS_KEY = "HCP_ACCESS_KEY";
	public static final String HCP_CURRENT_SECRET_KEY = "HCP_SECRET_KEY";

	//HCP and S3 READ Configuration
	public static final String HCP_READ_STORAGE_ID ="HCP_READ_STORAGE_ID";
	public static final String HCP_READ_REST_END_URL = "HCP_READ_REST_END_URL";
	public static final String HCP_READ_S3_END_URL = "HCP_READ_S3_END_URL";
	public static final String HCP_READ_BUCKET = "HCP_READ_BUCKET";
	public static final String HCP_READ_ACCESS_KEY = "HCP_READ_ACCESS_KEY";
	public static final String HCP_READ_SECRET_KEY = "HCP_READ_SECRET_KEY";
	
	
	public static final String HCP_DEAULT_CONFIG="HCP ";
	public static final String HCP_URL_ACCESS_KEY="{accessKey}";
	public static final String HCP_URL_SECRET_KEY="{SecretKey}";
	public static final String HCP_INDEX_FILE_NAME_PREFIX="_indx";
	
	public static final int  STORAGE_TYPE_CENTERA_ID=1;
	public static final int  STORAGE_TYPE_HCP_ID=2; 
	
	public static final int  STORAGE_CATEGORY_REGULATORY_R_W_ID=1;
	public static final int  STORAGE_CATEGORY_REGULATORY_R_ID=2;
	public static final int  STORAGE_CATEGORY_NON_REGULATORY_R_W_ID=3;
	public static final int  STORAGE_CATEGORY_NON_REGULATORY_R_ID=4;
	
	public static final String RETENTION_SCHEDULE_AUTOMATION = "RETENTION_SCHEDULE_AUTOMATION";
	public static final String RECORD_CODE_ALIGNMENT = "RECORD_CODE_ALIGNMENT";
	
	public static final String APPLY_RCA_BATCH_COUNT = "APPLY_RCA_BATCH_COUNT";
	public static final String APPLY_RCA_EMAIL_TO = "APPLY_RCA_EMAIL_TO";
	public static final String APPLY_RCA_EMAIL_FROM = "APPLY_RCA_EMAIL_FROM";
	public static final String APPLY_RCA_EMAIL_SUBJECT = "APPLY_RCA_EMAIL_SUBJECT";
	public static final String APPLY_RCA_EMAIL_REPORT_LINK = "APPLY_RCA_EMAIL_REPORT_LINK";
	public static final String APPLY_RCA_EMAIL_VALIDATE_LINK = "APPLY_RCA_EMAIL_VALIDATE_LINK";
	public static final String APPLY_RCA_RUN_MODE = "APPLY_RCA_RUN_MODE";
	public static final String APPLY_RCA_MAX_RUN_HOURS = "APPLY_RCA_MAX_RUN_HOURS";
	public static final String AIMS_RECORD_CODE_ALIGNMENT_UPDATE_CONFIRMATION_URL = "AIMS_RECORD_CODE_ALIGNMENT_UPDATE_CONFIRMATION_URL";
	public static final String SCHEDULED_RUN = "SCHEDULED_RUN";
	public static final String ONDEMAND_RUN = "ONDEMAND_RUN";

}
