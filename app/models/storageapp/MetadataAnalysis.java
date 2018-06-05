package models.storageapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import jobs.util.DatabaseConnection;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import com.avaje.ebean.Transaction;

import play.Logger;
import play.db.ebean.Model;
import utilities.Constant;



@Entity
@Table(name="ILM_METADATA_ANALYSIS")
public class MetadataAnalysis extends Model  {

	private static final long serialVersionUID = -7688571135588557278L;

	//@Id                              //Primary-Key
	//@SequenceGenerator(name="ANALYSISSEQ",sequenceName="ILM_METADATA_ANALYSIS_SEQ") 
	//@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ANALYSISSEQ")
	//@Column(name = "ID")
	//private Integer id;					//primary key
	
	@Id
	private String id = "";
	
	@Column(name="cycle_id")
	private Integer cycleId = 0;
	
	@Column(name="GUID")
	private String guid ="";			//storage app generated unique id AKA GUID	
	
	@Column(name="project_Id")
	private String projectId = "";	
	
	@Column(name="ait")
	private String ait = "";
	
	@Column(name="record_code_prev")
	private String recordCodePrev = "";	//from source - define by admin		
	
	@Column(name="country_code_prev")
	private String countryPrev = "";	
	
	@Column(name="record_code_new")
	private String recordCodeNew = "";	//from source - define by admin		
	
	@Column(name="country_code_new")
	private String countryNew = "";
	
	@Column(name="retention_end_date_prev")
	private Long retentionEndPrev = 0L;
	
	@Column(name="retention_end_date_new")
	private Long retentionEndNew = 0L;
	
	@Column(name="storage_uri_prev")
	private String storageUriPrev = "";			//storage URI Path

	@Column(name="storage_uri_new")
	private String storageUriNew = "";			//storage URI Path
	
	@Column(name="index_file_uri_prev")
	private String indexFileUriPrev = "";			

	@Column(name="index_file_uri_new")
	private String indexFileUriNew = "";			
	
	@Column(name="retention_type_prev")
	private String retentionTypePrev = "";			
	
	@Column(name="retention_type_new")
	private String retentionTypeNew = "";			
	
	@Column(name="db_update_flag")
	private String dbUpdateFlag = "N";	
	
	@Column(name="change_type")
	private String changeType = "";
	
	@Column(name="update_comment")
	private String ebrComment = "";
	
	@Column(name="update_type")
	private String updateType = "";

	protected String getId(){
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public Integer getCycleId() {
		return cycleId;
	}

	public void setCycleId(Integer cycleId) {
		this.cycleId = cycleId;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getAit() {
		return ait;
	}

	public void setAit(String ait) {
		this.ait = ait;
	}


	public String getRecordCodePrev() {
		return recordCodePrev;
	}
	public void setRecordCodePrev(String recordCodePrev) {
		this.recordCodePrev = recordCodePrev;
	}
	public String getCountryPrev() {
		return countryPrev;
	}
	public void setCountryPrev(String countryPrev) {
		this.countryPrev = countryPrev;
	}
	public String getRecordCodeNew() {
		return recordCodeNew;
	}
	public void setRecordCodeNew(String recordCodeNew) {
		this.recordCodeNew = recordCodeNew;
	}
	public String getCountryNew() {
		return countryNew;
	}
	public void setCountryNew(String countryNew) {
		this.countryNew = countryNew;
	}
	public String getUpdateType() {
		return updateType;
	}
	public void setUpdateType(String updateType) {
		this.updateType = updateType;
	}
	public Long getRetentionEndPrev() {
		return retentionEndPrev;
	}

	public void setRetentionEndPrev(Long retentionEndPrev) {
		this.retentionEndPrev = retentionEndPrev;
	}

	public Long getRetentionEndNew() {
		return retentionEndNew;
	}

	public void setRetentionEndNew(Long retentionEndNew) {
		this.retentionEndNew = retentionEndNew;
	}

	public String getStorageUriPrev() {
		return storageUriPrev;
	}

	public void setStorageUriPrev(String storageUriPrev) {
		this.storageUriPrev = storageUriPrev;
	}

	public String getStorageUriNew() {
		return storageUriNew;
	}

	public void setStorageUriNew(String storageUriNew) {
		this.storageUriNew = storageUriNew;
	}

	public String getIndexFileUriPrev() {
		return indexFileUriPrev;
	}
	
	public void setIndexFileUriPrev(String indexFileUriPrev) {
		this.indexFileUriPrev = indexFileUriPrev;
	}
	
	public String getIndexFileUriNew() {
		return indexFileUriNew;
	}
	
	public void setIndexFileUriNew(String indexFileUriNew) {
		this.indexFileUriNew = indexFileUriNew;
	}
	
	public String getRetentionTypePrev() {
		return retentionTypePrev;
	}

	public void setRetentionTypePrev(String retentionTypePrev) {
		this.retentionTypePrev = retentionTypePrev;
	}

	public String getRetentionTypeNew() {
		return retentionTypeNew;
	}

	public void setRetentionTypeNew(String retentionTypeNew) {
		this.retentionTypeNew = retentionTypeNew;
	}

	public String getDbUpdateFlag() {
		return dbUpdateFlag;
	}

	public void setDbUpdateFlag(String dbUpdateFlag) {
		this.dbUpdateFlag = dbUpdateFlag;
	}

	public String getChangeType() {
		return changeType;
	}

	public void setChangeType(String changeType) {
		this.changeType = changeType;
	}

	public String getEbrComment() {
		return ebrComment;
	}

	public void setEbrComment(String ebrComment) {
		this.ebrComment = ebrComment;
	}

	public static void saveAll(List<MetadataAnalysis> analysisData) {
		Ebean.save(analysisData);
		
/*		int batchCount =0;
		AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.APPLY_RETENTION_BATCH_COUNT);
   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
			Logger.warn("ApplyRetentionUpdatesJob - "+Constant.APPLY_RETENTION_BATCH_COUNT + " not found, running for batch coutn = 1000");
			batchCount =1000;
		}else{
			batchCount = Integer.parseInt(appConfigProperty.getValue());
		}
   		
		final Transaction tx = Ebean.beginTransaction();
		try{
			tx.setBatchMode(true);
			tx.setBatchSize(batchCount);
			//tx.setBatchGetGeneratedKeys(false);
			
			for(MetadataAnalysis m : analysisData){
				m.save();
			}
			Ebean.save(analysisData);
			Ebean.commitTransaction();  
			//tx.commit();
		}finally{
			Ebean.endTransaction();
			//tx.end();
		}*/
		
	}

	public static void truncate() {
		//Ebean.delete(all());
		
		/*Transaction t = Ebean.beginTransaction();
		Connection c = t.getConnection();
		try {
			c.createStatement().executeUpdate("truncate table ILM_METADATA_ANALYSIS");
			Ebean.commitTransaction();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			Ebean.endTransaction();
		}
		Ebean.getServerCacheManager().clearAll();*/
		
		Ebean.createSqlUpdate("truncate table ILM_METADATA_ANALYSIS").execute();
		int count = Ebean.createSqlQuery("select count(*) as c from ILM_METADATA_ANALYSIS").findUnique().getInteger("c");
		Logger.info("No of records in ILM_METADATA_ANALYSIS after truncating "+count);
		
	}
	
	//DB operations
	public static Finder<String, MetadataAnalysis> find = new Finder(String.class,
			MetadataAnalysis.class);

	public static List<MetadataAnalysis> all() {
		return find.all();
	}
	
	public static List<MetadataAnalysis> allWithDBUpdatedNo() {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("dbUpdateFlag", "N")
		        .findList();
		        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByAit(String ait) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("ait", ait).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByProjectId(String projectid) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("projectId", projectid).eq("dbUpdateFlag", "N")
		.findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByRecordCode( String recordCode) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("recordCodeNew", recordCode).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByAitAndProjectid(String ait, String projectid) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("ait", ait).eq("projectId", projectid).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByAitAndRecordCode( String ait, String recordCode) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("ait", ait).eq("recordCodeNew", recordCode).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByProjectidAndRecordCode( String projectid,String recordCode) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("projectId", projectid).eq("recordCodeNew", recordCode).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByAitAndProjectidAndRecordCode(String ait, String projectid,String recordCode) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().eq("ait", ait).eq("projectId", projectid).eq("recordCodeNew", recordCode).eq("dbUpdateFlag", "N")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataAnalysis> listByRecordCodes(List<String> recordCodes) {
		List<MetadataAnalysis> metadataList = MetadataAnalysis.find.where().in("recordCodeNew", recordCodes)
        .findList();
        return metadataList;
	}
	public static void insertAll(List<MetadataAnalysis> analysisData) throws Exception {
		Connection connectionObj = null;
		Statement stmt = null;
		ResultSet rs = null;
		int batchId = 0;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		
		try {
			Logger.info("Getting DB Connection");	
			connectionObj = DatabaseConnection.getUDASDBConnection() ;
			
			Logger.info("inserting into ILM_METADATA_ANALYSIS");
			connectionObj.setAutoCommit(false);

			String insertSQL = "INSERT INTO ILM_METADATA_ANALYSIS (ID, CYCLE_ID, GUID,PROJECT_ID, AIT, COUNTRY_CODE_NEW,"
					+ "RETENTION_END_DATE_PREV, RETENTION_END_DATE_NEW, STORAGE_URI_PREV, STORAGE_URI_NEW, RETENTION_TYPE_PREV, "
					+ "RETENTION_TYPE_NEW, DB_UPDATE_FLAG, UPDATE_COMMENT, RECORD_CODE_NEW, CHANGE_TYPE, INDEX_FILE_URI_PREV, INDEX_FILE_URI_NEW, UPDATE_TYPE, RECORD_CODE_PREV, COUNTRY_CODE_PREV) "
					+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			preparedStatement = connectionObj.prepareStatement(insertSQL);

			for(MetadataAnalysis a : analysisData){
				//Logger.info("preparing insert stmt for values= "+a.toString());
				preparedStatement.setString(1, a.getId());
				preparedStatement.setInt(2, a.getCycleId());
				preparedStatement.setString(3, a.getGuid());
				preparedStatement.setString(4, a.getProjectId());
				preparedStatement.setString(5, a.getAit());
				preparedStatement.setString(6, a.getCountryNew());
				if(null == a.getRetentionEndPrev()){
					preparedStatement.setNull(7,java.sql.Types.INTEGER);
				}else{
					preparedStatement.setLong(7, a.getRetentionEndPrev());
				}
				if(null == a.getRetentionEndNew()){
					preparedStatement.setNull(8,java.sql.Types.INTEGER);
				}else{
					preparedStatement.setLong(8, a.getRetentionEndNew());
				}
				preparedStatement.setString(9, a.getStorageUriPrev());
				preparedStatement.setString(10, a.getStorageUriNew());
				preparedStatement.setString(11, a.getRetentionTypePrev());
				preparedStatement.setString(12, a.getRetentionTypeNew());
				preparedStatement.setString(13, a.getDbUpdateFlag());
				preparedStatement.setString(14, a.getEbrComment());
				preparedStatement.setString(15, a.getRecordCodeNew());
				preparedStatement.setString(16, a.getChangeType());
				preparedStatement.setString(17, a.getIndexFileUriPrev());
				preparedStatement.setString(18, a.getIndexFileUriNew());
				preparedStatement.setString(19, a.getUpdateType());
				preparedStatement.setString(20, a.getRecordCodePrev());
				preparedStatement.setString(21, a.getCountryPrev());
				
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			connectionObj.commit();
		} catch (SQLException e) {
			Logger.error("Error while inserting ILM_METADATA_ANALYSIS into Db : "+e);
			e.printStackTrace();
			try {
				if(connectionObj != null)
					Logger.info("rolling back");
					connectionObj.rollback();
			} catch (SQLException e1) {
				Logger.error("Error while rollback inserting ILM_METADATA_ANALYSIS into Db : "+e);
				e1.printStackTrace();
			}
			throw new Exception(e);
		} catch (Exception e) {
			Logger.error("Error while inserting ILM_METADATA_ANALYSIS into Db : "+e);
			e.printStackTrace();
			throw new Exception(e);
		}finally{
			try {
				if(connectionObj != null)
					connectionObj.setAutoCommit(true);
			} catch (SQLException e) {
				Logger.error("Error while setting auto commit true : "+e);
				e.printStackTrace();
			}
			DatabaseConnection.close(rs);
			DatabaseConnection.close(resultSet);
			DatabaseConnection.close(stmt);
			DatabaseConnection.close(preparedStatement);
			DatabaseConnection.close(connectionObj); 
			Logger.info("Connection closed");
		}
		Logger.info("Exiting insertAll");
		
	}
	
	public static void updateAll(List<MetadataAnalysis> analysisData) throws Exception {
		Connection connectionObj = null;
		Statement stmt = null;
		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		
		try {
			Logger.info("Getting DB Connection");	
			connectionObj = DatabaseConnection.getUDASDBConnection() ;
			
			Logger.info("updating db update flag in ILM_METADATA_ANALYSIS");
			connectionObj.setAutoCommit(false);

			String updateSQL = "update ILM_METADATA_ANALYSIS set db_update_flag = ? where guid = ? and cycle_id = ?";
			preparedStatement = connectionObj.prepareStatement(updateSQL);

			for(MetadataAnalysis a : analysisData){
				preparedStatement.setString(1, a.getDbUpdateFlag());
				preparedStatement.setString(2, a.getGuid());
				preparedStatement.setInt(3, a.getCycleId());
			
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			connectionObj.commit();
		} catch (SQLException e) {
			Logger.error("Error while updating db update flag in ILM_METADATA_ANALYSIS in Db : "+e);
			e.printStackTrace();
			try {
				if(connectionObj != null)
					Logger.info("rolling back");
					connectionObj.rollback();
			} catch (SQLException e1) {
				Logger.error("Error while rollback updating db update flag in ILM_METADATA_ANALYSIS in Db : "+e);
				e1.printStackTrace();
			}
			throw new Exception(e);
		} catch (Exception e) {
			Logger.error("Error while updating db update flag in ILM_METADATA_ANALYSIS in Db : "+e);
			e.printStackTrace();
			throw new Exception(e);
		}finally{
			try {
				if(connectionObj != null)
					connectionObj.setAutoCommit(true);
			} catch (SQLException e) {
				Logger.error("Error while setting auto commit true : "+e);
				e.printStackTrace();
			}
			DatabaseConnection.close(rs);
			DatabaseConnection.close(resultSet);
			DatabaseConnection.close(stmt);
			DatabaseConnection.close(preparedStatement);
			DatabaseConnection.close(connectionObj); 
			Logger.info("Connection closed");
		}
		Logger.info("Exiting updateAll");
		
	}
	
}
