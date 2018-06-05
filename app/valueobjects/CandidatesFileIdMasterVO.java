package valueobjects;

import java.util.Date;

public class CandidatesFileIdMasterVO {
	
	private String candidatesFileId;
	private String expirationDate;
	private int processStatus;
	private Date creationDate;
	
	public String getCandidatesFileId() {
		return candidatesFileId;
	}
	public void setCandidatesFileId(String candidatesFileId) {
		this.candidatesFileId = candidatesFileId;
	}
	public String getExpirationDate() {
		return expirationDate;
	}
	public void setExpirationDate(String expirationDate) {
		this.expirationDate = expirationDate;
	}
	public int getProcessStatus() {
		return processStatus;
	}
	public void setProcessStatus(int processStatus) {
		this.processStatus = processStatus;
	}
	public Date getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
	
	

}
