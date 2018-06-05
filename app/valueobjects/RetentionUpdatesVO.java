package valueobjects;

import java.util.Date;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RetentionUpdatesVO {
	
	private Integer id = 0;  // pimary key of aims
	private String recordCode = "";
	private String countryCode = "";
	private String dispositionTimeUnit = "";
	private String usOfficialEventType = "";
	private Long usOfficialRetention = 0L;
	private String retentionStatus = "";
	private String usOfficialEventTypeHistory="";
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
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
	
	public String getUsOfficialEventType() {
		return usOfficialEventType;
	}
	
	public void setUsOfficialEventType(String usOfficialEventType) {
		this.usOfficialEventType = usOfficialEventType;
	}
		
	public Long getUsOfficialRetention() {
		return usOfficialRetention;
	}
	public void setUsOfficialRetention(Long usOfficialRetention) {
		this.usOfficialRetention = usOfficialRetention;
	}
	public String getRetentionStatus() {
		return retentionStatus;
	}
	
	public void setRetentionStatus(String retentionStatus) {
		this.retentionStatus = retentionStatus;
	}
	
	public String getDispositionTimeUnit() {
		return dispositionTimeUnit;
	}
	public void setDspositionTimeUnit(String dispositionTimeUnit) {
		this.dispositionTimeUnit = dispositionTimeUnit;
	}

	public String getUsOfficialEventTypeHistory() {
		return usOfficialEventTypeHistory;
	}
	public void setUsOfficialEventTypeHistory(String usOfficialEventTypeHistory) {
		this.usOfficialEventTypeHistory = usOfficialEventTypeHistory;
	}
	
	@Override
	public String toString() {
		return "RetentionUpdatesVO [aimsId=" + id
				+ ", recordCode=" + recordCode + ", countryCode="
				+ countryCode + ", dispositionTimeUnit="
				+ dispositionTimeUnit + ", usOfficialEventType=" + usOfficialEventType
				+ ", usOfficialRetention=" + usOfficialRetention
				+ ", retentionStatus=" + retentionStatus +"]";
	}
	

}
