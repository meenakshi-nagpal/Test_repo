package models.storageapp;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.IdClass;
import javax.persistence.Table;

import play.db.ebean.Model;

@Entity
@Table(name = "DESTRUCTION_CANDIDATE_LIST")
@IdClass(DestructionCandidateKey.class)
public class DestructionCandidates extends Model {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1294354266744356620L;

	@EmbeddedId
	private DestructionCandidateKey destructionCandidateKey;

	public DestructionCandidateKey getDestructionCandidateKey() {
		return destructionCandidateKey;
	}

	public void setDestructionCandidateKey(
			DestructionCandidateKey destructionCandidateKey) {
		this.destructionCandidateKey = destructionCandidateKey;
	}

	@Column(name = "IS_DELETED")
	private char isDeleted;

	@Column(name = "DELETION_TIMESTAMP")
	private Date deletionTimeStamp;

	@Column(name = "FAILURE_REASON_TEXT")
	private String failureReasonText;

	@Column(name = "FAILURE_REASON_CODE")
	private String failureReasonCode;

	public char getIsDeleted() {
		return isDeleted;
	}

	public void setIsDeleted(char isDeleted) {
		this.isDeleted = isDeleted;
	}

	public Date getDeletionTimeStamp() {
		return deletionTimeStamp;
	}

	public void setDeletionTimeStamp(Date deletionTimeStamp) {
		this.deletionTimeStamp = deletionTimeStamp;
	}

	public String getFailureReasonText() {
		return failureReasonText;
	}

	public void setFailureReasonText(String failureReasonText) {
		this.failureReasonText = failureReasonText;
	}

	public String getFailureReasonCode() {
		return failureReasonCode;
	}

	public void setFailureReasonCode(String failureReasonCode) {
		this.failureReasonCode = failureReasonCode;
	}

	public static void create(DestructionCandidates destructionCandidates) {
		destructionCandidates.save();
	}

	//DB operations
	public static Finder<DestructionCandidateKey,DestructionCandidates> find = new Finder<DestructionCandidateKey, DestructionCandidates> (
			DestructionCandidateKey.class, DestructionCandidates.class
	);

	public static List<DestructionCandidates> all() {
		return find.all();
	}
	
	public static List<DestructionCandidates> getReadyToDeleteFile(String id) 
	 {
	    return find.where()
	    		.eq("IS_DELETED", "N")
	    		.eq("RIMS_CANDIDATES_FILE_ID", id)
	    		.findList();
	 }
	
	public static List<DestructionCandidates> getCandidateFiles(String id) 
	 {
	    return find.where()
	    		.eq("RIMS_CANDIDATES_FILE_ID", id)
	    		.findList();
	 }
	
}
