package models.storageapp;

import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.SequenceGenerator;

import com.avaje.ebean.Ebean;

import play.db.ebean.Model;
import utilities.Constant;

@Entity
@Table(name="RETENTION_UPDATES")
public class RetentionUpdates extends Model{

	private static final long serialVersionUID = 154517482430239379L;

	@Id                              //Primary-Key
	//@TableGenerator(name="TABLE_GEN", table="SEQUENCE_TABLE", pkColumnName="SEQ_NAME", valueColumnName="SEQ_COUNT", pkColumnValue="EMP_SEQ")
    //@GeneratedValue(strategy=GenerationType.TABLE, generator="TABLE_GEN")
	//@SequenceGenerator(name="RETUPDATESEQ",sequenceName="ret_updates_seq") 
	//@GeneratedValue(strategy=GenerationType.AUTO, generator="RETUPDATESEQ")
	@Column(name = "ID")
	private Integer id = 0;

	@Column(name="CYCLE_ID")          //Unique-key
	private Integer cycleId = 0;

	@Column(name="AIMS_ID")
	private Integer aimsId = 0;
	
	@Column(name="RECORD_CODE")
	private String recordCode = "";

	@Column(name="COUNTRY_CODE")
	private String countryCode = null;
	
	@Column(name="US_OFFICIAL_EVENT_TYPE")
	private String eventType = "";
	
	@Column(name="US_OFFICIAL_EVENT_TYPE_HISTORY")
	private String usOfficialEventTypeHistory = "";
	
	@Column(name="US_OFFICIAL_RETENTION")
	private Long retention = 0L;
	
	@Column(name="DISPOSITION_TIMEUNIT")
	private String timeUnit = "";
	
	@Column(name="RETENTION_STATUS")
	private String retentionStatus = "";
	
	@Column(name="AIMS_RC_UPDATES_RECEIVED") 
	private String isAimsAlertNotified = "N";
	
	@Column(name="AIMS_RC_UPDATES_CONFIRMED") 
	private String isAimsConfirmationNotified = "N";

		
	public Integer getId(){
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	
	public Integer getCycleId() {
		return cycleId;
	}

	public void setCycleId(Integer cycleId) {
		this.cycleId = cycleId;
	}
	
	public Integer getAimsId() {
		return aimsId;
	}

	public void setAimsId(Integer aimsId) {
		this.aimsId = aimsId;
	}
	
	public String getRecordCode() {
		return recordCode;
	}
	
	public void setRecordCode(String recordCode) {
		this.recordCode = recordCode;
	}
	
	public String getCountryCode() {
		return countryCode;
	}
	
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	
	public String getEventType() {
		return eventType;
	}
	
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
		
	public Long getRetention() {
		return retention;
	}
	public void setRetention(Long retention) {
		this.retention = retention;
	}
	public String getRetentionStatus() {
		return retentionStatus;
	}
	
	public void setRetentionStatus(String retentionStatus) {
		this.retentionStatus = retentionStatus;
	}
	
	
	public String getTimeUnit() {
		return timeUnit;
	}
	public void setTimeUnit(String timeUnit) {
		this.timeUnit = timeUnit;
	}
	
	public String getUsOfficialEventTypeHistory() {
		return usOfficialEventTypeHistory;
	}
	public void setUsOfficialEventTypeHistory(String usOfficialEventTypeHistory) {
		this.usOfficialEventTypeHistory = usOfficialEventTypeHistory;
	}

	public String getIsAimsAlertNotified() {
		return isAimsAlertNotified;
	}
	
	public void setIsAimsAlertNotified(String isAimsAlertNotified) {
		this.isAimsAlertNotified = isAimsAlertNotified;
	}
	
	public String getIsAimsConfirmationNotified() {
		return isAimsConfirmationNotified;
	}
	
	public void setIsAimsConfirmationNotified(String isAimsConfirmationNotified) {
		this.isAimsConfirmationNotified = isAimsConfirmationNotified;
	}
	
	
	//DB operations
	public static Finder<String, RetentionUpdates> find = new Finder(String.class,
			RetentionUpdates.class);

	public static List<RetentionUpdates> all() {
		return find.all();
	}
	
	public static List<RetentionUpdates> listByCycleId(Integer cycleId) {
		List<RetentionUpdates> retentionUpdatesList = RetentionUpdates.find.where().eq("cycleId", cycleId)
        .findList();
        return retentionUpdatesList;
	}
	
/*	public static List<Metadata> listMetadataByCycleId(Integer cycleId) {
		List<RetentionUpdates> retentionUpdatesList = RetentionUpdates.find.where().eq("cycleId", cycleId)
        .findList();
		List<Metadata> data = new ArrayList<Metadata>();
		for(RetentionUpdates ru : retentionUpdatesList){
			data.addAll(ru.metadata);
		}
		
        return data;
	}*/
	public static void saveAll(List<RetentionUpdates> retentionUpdates) {
		Ebean.save(retentionUpdates);
	}
	
	
	public static RetentionUpdates findByCycleIdAndRecordCodeAndCountryCode(String recordCode, String countryCode, Integer cycleId)
	{
		RetentionUpdates retUpdate = (RetentionUpdates)find.where().eq("recordCode", recordCode).eq("countryCode", countryCode).eq("cycleId", cycleId).findUnique();
		return retUpdate;
	}
	
	public static RetentionUpdates findByAimsId(Integer aimsId)
	{
		RetentionUpdates retentionUpdates = (RetentionUpdates)find.where().eq("aimsId", aimsId).findUnique();
		return retentionUpdates;
	}
	
	public static List<RetentionUpdates> pendingAimsAlert() {
		List<RetentionUpdates> retentionPendingAlertList = RetentionUpdates.find.where().eq("isAimsAlertNotified", Constant.falseValue)
        .findList();
        return retentionPendingAlertList;
	}
	
	public static Integer getGeneratedID() {
		Integer id=0;
		RetentionUpdates retentionUpdates = RetentionUpdates.find.setMaxRows(1).orderBy("ID desc").findUnique();
		if(retentionUpdates==null){
			id=id+1;
		}
		else
		{
		 id=retentionUpdates.getId()+1;	
		}
        return id;
	}
	
	@Override
	public String toString() {
		return "RetentionUpdates [id=" + id
				+ ", cycleId=" + cycleId + ", aimsId="
				+ aimsId + ", recordCode="
				+ recordCode + ", countryCode=" + countryCode
				+ ", eventType=" + eventType
				+ ", retention=" + retention + ", timeUnit="
				+ timeUnit+", retentionStatus="+retentionStatus+" ]";
	}
	
	public static List<RetentionUpdates> getAllConfirmationNotSent() {
		List<RetentionUpdates> retentionPendingConfList = RetentionUpdates.find.where().eq("isAimsConfirmationNotified", Constant.falseValue)
		        .findList();
		return retentionPendingConfList;
	}
	
}