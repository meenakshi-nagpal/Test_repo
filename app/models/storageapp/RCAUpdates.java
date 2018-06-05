package models.storageapp;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.avaje.ebean.Ebean;

import play.db.ebean.Model;
import utilities.Constant;

@Entity
@Table(name="RCA_UPDATES")
public class RCAUpdates extends Model{

	private static final long serialVersionUID = 7164774718990578144L;

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
	
	@Column(name="AIT")
	private String ait = "";
	
	@Column(name="PROJECT_ID")
	private String projectId = "";
	
	@Column(name="RECORD_CODE_PREV")
	private String recordCodePrev = "";

	@Column(name="COUNTRY_CODE_PREV")
	private String countryCodePrev = null;
	
	@Column(name="RECORD_CODE_NEW")
	private String recordCodeNew = "";

	@Column(name="COUNTRY_CODE_NEW")
	private String countryCodeNew = null;
	
	@Column(name="US_OFFICIAL_EVENT_TYPE_PREV")
	private String eventTypePrev = "";
	
	@Column(name="US_OFFICIAL_EVENT_TYPE_NEW")
	private String eventTypeNew = "";
	
	@Column(name="US_OFFICIAL_RETENTION_NEW")
	private Long retention = 0L;
	
	@Column(name="DISPOSITION_TIMEUNIT_NEW")
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
		
	public String getRecordCodePrev() {
		return recordCodePrev;
	}
	public void setRecordCodePrev(String recordCodePrev) {
		this.recordCodePrev = recordCodePrev;
	}
	public String getCountryCodePrev() {
		return countryCodePrev;
	}
	public void setCountryCodePrev(String countryCodePrev) {
		this.countryCodePrev = countryCodePrev;
	}
	public String getRecordCodeNew() {
		return recordCodeNew;
	}
	public void setRecordCodeNew(String recordCodeNew) {
		this.recordCodeNew = recordCodeNew;
	}
	public String getCountryCodeNew() {
		return countryCodeNew;
	}
	public void setCountryCodeNew(String countryCodeNew) {
		this.countryCodeNew = countryCodeNew;
	}
	public String getEventTypePrev() {
		return eventTypePrev;
	}
	public void setEventTypePrev(String eventTypePrev) {
		this.eventTypePrev = eventTypePrev;
	}
	public String getEventTypeNew() {
		return eventTypeNew;
	}
	public void setEventTypeNew(String eventTypeNew) {
		this.eventTypeNew = eventTypeNew;
	}


	public String getAit() {
		return ait;
	}
	public void setAit(String ait) {
		this.ait = ait;
	}
	public String getProjectId() {
		return projectId;
	}
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}


	//DB operations
	public static Finder<String, RCAUpdates> find = new Finder(String.class,
			RCAUpdates.class);

	public static List<RCAUpdates> all() {
		return find.all();
	}
	
	public static List<RCAUpdates> listByCycleId(Integer cycleId) {
		List<RCAUpdates> RCAUpdatesList = RCAUpdates.find.where().eq("cycleId", cycleId)
        .findList();
        return RCAUpdatesList;
	}
	
/*	public static List<Metadata> listMetadataByCycleId(Integer cycleId) {
		List<RCAUpdates> RCAUpdatesList = RCAUpdates.find.where().eq("cycleId", cycleId)
        .findList();
		List<Metadata> data = new ArrayList<Metadata>();
		for(RCAUpdates ru : RCAUpdatesList){
			data.addAll(ru.metadata);
		}
		
        return data;
	}*/
	public static void saveAll(List<RCAUpdates> RCAUpdates) {
		Ebean.save(RCAUpdates);
	}
	
	
	public static RCAUpdates findByCycleIdAndRecordCodeAndCountryCode(String recordCode, String countryCode, Integer cycleId)
	{
		RCAUpdates retUpdate = (RCAUpdates)find.where().eq("recordCode", recordCode).eq("countryCode", countryCode).eq("cycleId", cycleId).findUnique();
		return retUpdate;
	}
	
	public static RCAUpdates findByAimsId(Integer aimsId)
	{
		RCAUpdates RCAUpdates = (RCAUpdates)find.where().eq("aimsId", aimsId).findUnique();
		return RCAUpdates;
	}
	
	public static List<RCAUpdates> pendingAimsAlert() {
		List<RCAUpdates> retentionPendingAlertList = RCAUpdates.find.where().eq("isAimsAlertNotified", Constant.falseValue)
        .findList();
        return retentionPendingAlertList;
	}
	
	public static Integer getGeneratedID() {
		Integer id=0;
		RCAUpdates rcaUpdates = RCAUpdates.find.setMaxRows(1).orderBy("ID desc").findUnique();
		if(rcaUpdates==null){
			id=id+1;
		}
		else
		{
		 id=rcaUpdates.getId()+1;	
		}
        return id;
	}
	@Override
	public String toString() {
		return "RCAUpdates [id=" + id + ", cycleId=" + cycleId + ", aimsId="
				+ aimsId + ", ait=" + ait + ", projectId=" + projectId
				+ ", recordCodePrev=" + recordCodePrev + ", countryCodePrev="
				+ countryCodePrev + ", recordCodeNew=" + recordCodeNew
				+ ", countryCodeNew=" + countryCodeNew + ", eventTypePrev="
				+ eventTypePrev + ", eventTypeNew=" + eventTypeNew
				+ ", retention=" + retention + ", timeUnit=" + timeUnit
				+ ", retentionStatus=" + retentionStatus
				+ ", isAimsAlertNotified=" + isAimsAlertNotified
				+ ", isAimsConfirmationNotified=" + isAimsConfirmationNotified
				+ "]";
	}
	public static List<RCAUpdates> getAllConfirmationNotSent() {
		List<RCAUpdates> retentionPendingConfList = RCAUpdates.find.where().eq("isAimsConfirmationNotified", Constant.falseValue)
		        .findList();
		return retentionPendingConfList;
	}

		
}