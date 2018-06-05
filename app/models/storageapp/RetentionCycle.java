package models.storageapp;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

import play.db.ebean.Model;


@Entity
@Table(name="RETENTION_CYCLE")
public class RetentionCycle extends Model{

	private static final long serialVersionUID = 884085628964386480L;

	@Id                              //Primary-Key
	//@SequenceGenerator(name="RETCYCSEQ",sequenceName="ret_cycle_seq") 
	//@GeneratedValue(strategy=GenerationType.AUTO, generator="RETCYCSEQ")
	@Column(name = "CYCLE_ID")
	private Integer cycleId;

	@Column(name="CYCLE_STATUS_ID")
	private Integer currentStatus = 0;

	@Column(name="CYCLE_DATETIME")
	@JsonFormat(pattern="yyyy-MM-dd")
	private Date cycleDate;
	
	@Column(name="EXECUTION_MODE")
	private String executionMode = "";
	
	@Column(name="ANALYSIS_VALIDATION_MODE")
	private String analysisValidationMode = "";
	
	public Integer getCycleId(){
		return cycleId;
	}
	public void setCycleId(Integer cycleId) {
		this.cycleId = cycleId;
	}
	

	public Integer getCurrentStatus() {
		return currentStatus;
	}
	
	public void setCurrentStatus(Integer currentStatus) {
		this.currentStatus = currentStatus;
	}
	
	public Date getCycleDate() {
		return cycleDate;
	}
	
	public void setCycleDate(Date cycleDate) {
		this.cycleDate = cycleDate;
	}
	
	public String getExecutionMode() {
		return executionMode;
	}
	public void setExecutionMode(String executionMode) {
		this.executionMode = executionMode;
	}
	public String getAnalysisValidationMode() {
		return analysisValidationMode;
	}
	public void setAnalysisValidationMode(String analysisValidationMode) {
		this.analysisValidationMode = analysisValidationMode;
	}

	public static Finder<String, RetentionCycle> find = new Finder(String.class,RetentionCycle.class);
	
	public static RetentionCycle getCurrentCycleDetails(){
		
		RetentionCycle retentionCycle = RetentionCycle.find.setMaxRows(1).orderBy("cycleId desc").findUnique();
		if(retentionCycle==null){
			return null;
		}
		return  retentionCycle;
	}
	
	public static Integer getGeneratedCycleID() {
		Integer cycleID=0;
		RetentionCycle retentionCycle = RetentionCycle.find.setMaxRows(1).orderBy("cycleId desc").findUnique();
		if(retentionCycle==null){
			cycleID=cycleID+1;
		}
		else
		{
			cycleID=retentionCycle.getCycleId()+1;	
		}
        return cycleID;
	}
	@Override
	public String toString() {
		return "RetentionCycle [cycleId=" + cycleId + ", currentStatus="
				+ currentStatus + ", cycleDate=" + cycleDate
				+ ", executionMode=" + executionMode
				+ ", analysisValidationMode=" + analysisValidationMode + "]";
	}

	
}