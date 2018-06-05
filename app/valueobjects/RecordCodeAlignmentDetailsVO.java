package valueobjects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordCodeAlignmentDetailsVO {
	
	private Integer id = 0;  // pimary key of aims
	private String ait="";
	private String recordCode = "";
	private String recordCodeHistory="";
	private String countryCode = "";
	private String countryCodeHistory= "";
	private String projectId="";
	private String retentionStatus="";
	private String dispositionTimeUnit = "";
	private String usOfficialEventType = "";
	private String usOfficialEventTypeHistory="";
	private Long usOfficialRetention = 0L;
	private Long usOfficialRetentionHistory= 0L;
	
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
	
	public String getAit() {
		return ait;
	}
	
	public void setAit(String ait) {
		this.ait = ait;
	}
	
	public String getRecordCode() {
		return recordCode;
	}
	
	public void setRecordCode(String recordCode) {
		this.recordCode = recordCode;
	}
	
	public String getRecordCodeHistory() {
		return recordCodeHistory;
	}
	
	public void setRecordCodeHistory(String recordCodeHistory) {
		this.recordCodeHistory = recordCodeHistory;
	}
	
	
	public String getCountryCode() {
		return countryCode;
	}
	
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	
	public String getCountryCodeHistory() {
		return countryCodeHistory;
	}
	
	public void setCountryCodeHistory(String countryCodeHistory) {
		this.countryCodeHistory = countryCodeHistory;
	}
	
	public String getProjectId() {
		return projectId;
	}
	
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	
	public String getRetentionStatus() {
		return retentionStatus;
	}
	
	public void setRetentionStatus(String retentionStatus) {
		this.retentionStatus = retentionStatus;
	}

		
	public String getUsOfficialEventType() {
		return usOfficialEventType;
	}
	
	public void setUsOfficialEventType(String usOfficialEventType) {
		this.usOfficialEventType = usOfficialEventType;
	}
		
	public String getUsOfficialEventTypeHistory() {
		return usOfficialEventTypeHistory;
	}
	
	public void setUsOfficialEventTypeHistory(String usOfficialEventTypeHistory) {
		this.usOfficialEventTypeHistory = usOfficialEventTypeHistory;
	}
	
	public Long getUsOfficialRetention() {
		return usOfficialRetention;
	}
	public void setUsOfficialRetention(Long usOfficialRetention) {
		this.usOfficialRetention = usOfficialRetention;
	}
	
	public Long getUsOfficialRetentionHistory() {
		return usOfficialRetentionHistory;
	}
	public void setUsOfficialRetentionHistory(Long usOfficialRetentionHistory) {
		this.usOfficialRetentionHistory = usOfficialRetentionHistory;
	}
	
	public String getDispositionTimeUnit() {
		return dispositionTimeUnit;
	}
	public void setDspositionTimeUnit(String dispositionTimeUnit) {
		this.dispositionTimeUnit = dispositionTimeUnit;
	}

		
	@Override
	public String toString() {
		return "RetentionUpdatesVO [aimsId=" + id
				+ ", recordCode=" + recordCode + ", recordCodeHistory="+ recordCodeHistory + ", countryCode="
				+ countryCode  + ", countryCodeHistory="+ countryCodeHistory + ", dispositionTimeUnit="
				+ dispositionTimeUnit + ", usOfficialEventType=" + usOfficialEventType
				+ ", usOfficialRetention=" + usOfficialRetention
				+ ", retentionStatus=" + retentionStatus + ", projectId="+ projectId + ", usOfficialEventTypeHistory="
				+ usOfficialEventTypeHistory  + ", usOfficialRetentionHistory=" + usOfficialRetentionHistory +"]";
	}
	

}
