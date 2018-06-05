package models.storageapp;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import java.io.Serializable;

@Embeddable
public class DestructionCandidateKey implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8162227868089314233L;
	
	@Column(name = "RIMS_CANDIDATES_FILE_ID")
	private String candidateFileId;
	
	@Column(name = "AIMS_GUID")
	private String aimsGuid;
	
	DestructionCandidateKey() {}

	public DestructionCandidateKey(String candidateFileId, String aimsGuid) {
        this.candidateFileId = candidateFileId;
        this.aimsGuid = aimsGuid;
    }

	public String getCandidateFileId() {
		return candidateFileId;
	}

	public void setCandidateFileId(String candidateFileId) {
		this.candidateFileId = candidateFileId;
	}

	public String getAimsGuid() {
		return aimsGuid;
	}

	public void setAimsGuid(String aimsGuid) {
		this.aimsGuid = aimsGuid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((aimsGuid == null) ? 0 : aimsGuid.hashCode());
		result = prime * result
				+ ((candidateFileId == null) ? 0 : candidateFileId.hashCode());
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
		DestructionCandidateKey other = (DestructionCandidateKey) obj;
		if (aimsGuid == null) {
			if (other.aimsGuid != null)
				return false;
		} else if (!aimsGuid.equals(other.aimsGuid))
			return false;
		if (candidateFileId == null) {
			if (other.candidateFileId != null)
				return false;
		} else if (!candidateFileId.equals(other.candidateFileId))
			return false;
		return true;
	}
}
