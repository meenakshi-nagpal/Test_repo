package valueobjects;

public class ArchiveFileRegistrationVO {

	private String projectId;
	private String aimsGuid;
	private String lobArchiveFileId;
	private String retentionStartDate;
	private Long archiveSizeInBytes;
	private Long archiveRows;
	private String archiveDescription;
	private String lobIndexFileId;
	private Long indexFileSize;
	private Integer archiveFileStatus;
	private String registrationDate;
	private String registeredBy;
	private String modificationDate;
	private String modifiedBy;
	private String userAgent;
	private Character legalHold;
	private Boolean userErrorFlag;
	private String archiveDate;
	private String eventBased;
	
	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getAimsGuid() {
		return aimsGuid;
	}

	public void setAimsGuid(String aimsGuid) {
		this.aimsGuid = aimsGuid;
	}

	public String getLobArchiveFileId() {
		return lobArchiveFileId;
	}

	public void setLobArchiveFileId(String lobArchiveFileId) {
		this.lobArchiveFileId = lobArchiveFileId;
	}

	public String getRetentionStartDate() {
		return retentionStartDate;
	}

	public void setRetentionStartDate(String retentionStartDate) {
		this.retentionStartDate = retentionStartDate;
	}

	public Long getArchiveSizeInBytes() {
		return archiveSizeInBytes;
	}

	public void setArchiveSizeInBytes(Long archiveSizeInBytes) {
		this.archiveSizeInBytes = archiveSizeInBytes;
	}

	public Long getArchiveRows() {
		return archiveRows;
	}

	public void setArchiveRows(Long archiveRows) {
		this.archiveRows = archiveRows;
	}

	public String getArchiveDescription() {
		return archiveDescription;
	}

	public void setArchiveDescription(String archiveDescription) {
		this.archiveDescription = archiveDescription;
	}

	public String getLobIndexFileId() {
		return lobIndexFileId;
	}

	public void setLobIndexFileId(String lobIndexFileId) {
		this.lobIndexFileId = lobIndexFileId;
	}

	public Long getIndexFileSize() {
		return indexFileSize;
	}

	public void setIndexFileSize(Long indexFileSize) {
		this.indexFileSize = indexFileSize;
	}

	public Integer getArchiveFileStatus() {
		return archiveFileStatus;
	}

	public void setArchiveFileStatus(Integer archiveFileStatus) {
		this.archiveFileStatus = archiveFileStatus;
	}

	public String getRegistrationDate() {
		return registrationDate;
	}

	public void setRegistrationDate(String registrationDate) {
		this.registrationDate = registrationDate;
	}

	public String getRegisteredBy() {
		return registeredBy;
	}

	public void setRegisteredBy(String registeredBy) {
		this.registeredBy = registeredBy;
	}

	public String getModificationDate() {
		return modificationDate;
	}

	public void setModificationDate(String modificationDate) {
		this.modificationDate = modificationDate;
	}

	public String getModifiedBy() {
		return modifiedBy;
	}

	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public Character getLegalHold() {
		return legalHold;
	}

	public void setLegalHold(Character legalHold) {
		this.legalHold = legalHold;
	}

	public Boolean getUserErrorFlag() {
		return userErrorFlag;
	}

	public void setUserErrorFlag(Boolean userErrorFlag) {
		this.userErrorFlag = userErrorFlag;
	}

	public String getArchiveDate() {
		return archiveDate;
	}

	public void setArchiveDate(String archiveDate) {
		this.archiveDate = archiveDate;
	}

	
	public String getEventBased() {
		return eventBased;
	}

	public void setEventBased(String eventBased) {
		this.eventBased = eventBased;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((aimsGuid == null) ? 0 : aimsGuid.hashCode());
		result = prime * result
				+ ((projectId == null) ? 0 : projectId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArchiveFileRegistrationVO other = (ArchiveFileRegistrationVO) obj;
		if (aimsGuid == null) {
			if (other.aimsGuid != null)
				return false;
		} else if (!aimsGuid.equals(other.aimsGuid))
			return false;
		if (projectId == null) {
			if (other.projectId != null)
				return false;
		} else if (!projectId.equals(other.projectId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ArchiveFileRegistrationVO [projectId=" + projectId
				+ ", aimsGuid=" + aimsGuid + ", lobArchiveFileId="
				+ lobArchiveFileId + ", retentionStartDate="
				+ retentionStartDate + ", archiveSizeInBytes="
				+ archiveSizeInBytes + ", archiveRows=" + archiveRows
				+ ", archiveDescription=" + archiveDescription
				+ ", lobIndexFileId=" + lobIndexFileId + ", indexFileSize="
				+ indexFileSize + ", archiveFileStatus=" + archiveFileStatus
				+ ", registrationDate=" + registrationDate + ", registeredBy="
				+ registeredBy + ", modificationDate=" + modificationDate
				+ ", modifiedBy=" + modifiedBy + ", userAgent=" + userAgent
				+ ", legalHold=" + legalHold + ", userErrorFlag="
				+ userErrorFlag + ", eventBased=" +eventBased
				+ " ]";
	}
	
	

}
