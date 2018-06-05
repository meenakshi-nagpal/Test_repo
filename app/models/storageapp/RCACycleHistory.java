package models.storageapp;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import play.db.ebean.Model;

@Entity
@Table(name="RCA_CYCLE_HISTORY")
public class RCACycleHistory extends Model{

	private static final long serialVersionUID = 4942061648042318608L;

	@Id                              //Primary-Key
	//@SequenceGenerator(name="RETCYCHISTORYSEQ",sequenceName="ret_cyclehist_seq") 
	//@GeneratedValue(strategy=GenerationType.AUTO, generator="RETCYCHISTORYSEQ")
	@Column(name = "ID")
	private Integer id;
		
	@Column(name = "CYCLE_ID")
	private Integer cycleId = 0;

	@Column(name="STATUS_ID")
	private Integer status = 0;

	@Column(name="CYCLE_DATETIME")
	private Date cycleDate;
	
	@Column(name="EXECUTION_MODE")
	private String executionMode = "";
	
	@Column(name="ANALYSIS_VALIDATION_MODE")
	private String analysisValidationMode = "";
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
	public Integer getCycleId(){
		return cycleId;
	}
	
	public void setCycleId(Integer cycleId) {
		this.cycleId = cycleId;
	}
		
	public Integer getStatus() {
		return status;
	}
	
	public void setStatus(Integer status) {
		this.status = status;
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


		//DB operations
		public static Finder<String, RCACycleHistory> find = new Finder(String.class,
				RCACycleHistory.class);
		
		
	public static Integer getGeneratedRCACycleHistoryId() {
		Integer ID=0;
		RCACycleHistory rcaCycleHistory = RCACycleHistory.find.setMaxRows(1).orderBy("ID desc").findUnique();
		if(rcaCycleHistory==null){
			ID=ID+1;
		}
		else
		{
			ID=rcaCycleHistory.getId()+1;	
		}
        return ID;
	}
	
}