package models.storageapp;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import play.db.ebean.Model;
import utilities.Constant;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;

@Entity
@Table(name = "CANDIDATE_LIST_MASTER")
public class CandidatesFileId extends Model {

	private static final long serialVersionUID = -7034034381095114925L;
	
	@Id
	@Column(name = "RIMS_CANDIDATES_FILE_ID")
	private String id;
	
	@Column(name = "PROCESS_STATUS")
	private Integer processStatus;
	
	@Column(name = "EXPIRATION_DATE")
	private Date expirationDate;
	
	@Column(name = "CREATION_TIMESTAMP")
	private Timestamp creationDate;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getProcessStatus() {
		return processStatus;
	}

	public void setProcessStatus(Integer processStatus) {
		this.processStatus = processStatus;
	}

	public Date getExpirationDate() {
		return expirationDate;
	}

	public void setExpirationDate(Date expirationDate) {
		this.expirationDate = expirationDate;
	}

	public Timestamp getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	
	public static CandidatesFileId findById(String candidatesFileId) {
		return Ebean.find(CandidatesFileId.class, candidatesFileId);
	}
	
	public static Finder<Long, CandidatesFileId> find = new Finder(Long.class,
			CandidatesFileId.class);
	
	public static List<CandidatesFileId> getCandidatesFileList(Integer status){
		List<CandidatesFileId> candidatesFileList = CandidatesFileId.find.where().eq("PROCESS_STATUS", status).findList();
		return 	candidatesFileList;
		
	}
	
	public static List<CandidatesFileId> getCandidatesFileToDelete(){
		
		List<CandidatesFileId> candidatesFileList = find
				.where().or(Expr.eq("PROCESS_STATUS", Constant.CANDIDATES_PROCESS_STATUS_RECEIVED), Expr.eq("PROCESS_STATUS", Constant.CANDIDATES_PROCESS_STATUS_PENDING))
				.gt("EXPIRATION_DATE", utilities.Utility.getSafeExpiration())
				.findList();
		return 	candidatesFileList;
	}
	
	public static List<CandidatesFileId> getExpiredCandidatesFiles(){
		
		List<CandidatesFileId> candidatesFileList = find
				.where().eq("PROCESS_STATUS", Constant.CANDIDATES_PROCESS_STATUS_PENDING)
				.le("EXPIRATION_DATE", utilities.Utility.getSafeExpiration())
				.findList();
		return 	candidatesFileList;
	}
	
}
