package models.storageapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import jobs.util.DatabaseConnection;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlQuery;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.Transaction;
import com.avaje.ebean.SqlUpdate;

import play.Logger;
import play.db.ebean.Model;
import utilities.Constant;
import utilities.Utility;


@Entity
@Table(name="ILM_METADATA_HISTORY")
public class MetadataHistory extends Model  {


	private static final long serialVersionUID = -3572900593372722023L;

/*	@Id                              //Primary-Key
	@SequenceGenerator(name="HISTORYSEQ",sequenceName="ILM_METADATA_HISTORY_SEQ") 
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="HISTORYSEQ")
	@Column(name = "ID")
	private Integer id = 0;	*/				//primary key
	
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
	
	@Column(name="record_code_new")
	private String recordCodeNew = "";	//from source - define by admin		
	
	@Column(name="country_code_new")
	private String countryNew = "";	
	
	@Column(name="record_code_prev")
	private String recordCodePrev = "";	//from source - define by admin		
	
	@Column(name="country_code_prev")
	private String countryPrev = "";	
	
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

	@Column(name="create_timestamp")
	private Long createTimestamp = 0L;
	
	@Column(name="storage_uri_prev_status")
	private String storageUriPrevStatus = "";	
	
	@Column(name="EXTENSION_STATUS")
	private String extensionStatus = "";
	
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
	
	public Long getCreateTimestamp() {
		return createTimestamp;
	}
	public void setCreateTimestamp(Long createTimestamp) {
		this.createTimestamp = createTimestamp;
	}

	public String getStorageUriPrevStatus() {
		return storageUriPrevStatus;
	}

	public void setStorageUriPrevStatus(String storageUriPrevStatus) {
		this.storageUriPrevStatus = storageUriPrevStatus;
	}
	
	public String getExtensionStatus() {
		return extensionStatus;
	}
	public void setExtensionStatus(String extensionStatus) {
		this.extensionStatus = extensionStatus;
	}
	
	//DB operations
	public static Finder<String, MetadataHistory> find = new Finder(String.class,
			MetadataHistory.class);

	public static List<MetadataHistory> all() {
		return find.all();
	}

	public static List<MetadataHistory> listByGuidOrderByRetentionEndDateNewDesc(String guid) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("guid", guid).orderBy("retention_end_date_new desc")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataHistory> listByGuid(String guid) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("guid", guid)
        .findList();
        return metadataList;
	}

	public static List<MetadataHistory> listByGuids(List<String> guids) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().in("guid", guids).orderBy("guid")
        .findList();
        return metadataList;
	}
	
	public static List<MetadataHistory> listByGuidOrderByCreateTimestamp(String guid) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("guid", guid).orderBy("createTimestamp")
        .findList();
        return metadataList;
	}

	
	public static List<MetadataHistory> listByGuidAndCycleId(List<String> guids, Integer cycleId) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("cycleId", cycleId).in("guid",guids)
		.findList();
		return metadataList;
	}

	public static void saveAll(List<MetadataHistory> updatedHistoryData) {
		Ebean.save(updatedHistoryData);
		
	}
	
	public static List<MetadataHistory> listByAit(Integer cycleId,String ait)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByProjectId(Integer cycleId, String projectid)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByRecordCode(Integer cycleId, String recordCode)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByAitAndProjectid(Integer cycleId, String ait, String projectid)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByAitAndRecordCode(Integer cycleId, String ait, String recordCode)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByProjectidAndRecordCode(Integer cycleId, String projectid,String recordCode)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
		return metadataList;
	}
	
	public static List<MetadataHistory> listByAitAndProjectidAndRecordCode(Integer cycleId, String ait, String projectid,String recordCode) throws Exception {
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
		return metadataList;
	}

	public static void insertAll(List<MetadataHistory> historyData) throws Exception {
		Connection connectionObj = null;
		Statement stmt = null;
		PreparedStatement preparedStatement = null;
		
		try {
			Logger.info("Getting DB Connection");	
			connectionObj = DatabaseConnection.getUDASDBConnection() ;
			
			Logger.info("inserting into ILM_METADATA_HISTORY");
			connectionObj.setAutoCommit(false);

			String insertSQL = "INSERT INTO ILM_METADATA_HISTORY (ID, CYCLE_ID, GUID, PROJECT_ID, AIT, COUNTRY_CODE_NEW, RETENTION_END_DATE_PREV, "
					+ "RETENTION_END_DATE_NEW, STORAGE_URI_PREV, STORAGE_URI_NEW, RETENTION_TYPE_PREV, RETENTION_TYPE_NEW, DB_UPDATE_FLAG, UPDATE_COMMENT, RECORD_CODE_NEW, "
					+ "CHANGE_TYPE, INDEX_FILE_URI_PREV, INDEX_FILE_URI_NEW, CREATE_TIMESTAMP,EXTENSION_STATUS, UPDATE_TYPE, RECORD_CODE_PREV, COUNTRY_CODE_PREV) "
					+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			preparedStatement = connectionObj.prepareStatement(insertSQL);

			for(MetadataHistory a : historyData){
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
				preparedStatement.setLong(19, Utility.getCurrentTime());
				preparedStatement.setString(20, a.getExtensionStatus());
				preparedStatement.setString(21, a.getUpdateType());
				preparedStatement.setString(22, a.getRecordCodePrev());
				preparedStatement.setString(23, a.getCountryPrev());
				
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			connectionObj.commit();
		} catch (SQLException e) {
			Logger.error("Error while inserting ILM_METADATA_HISTORY into Db : "+e);
			e.printStackTrace();
			try {
				if(connectionObj != null)
					Logger.info("rolling back");
					connectionObj.rollback();
			} catch (SQLException e1) {
				Logger.error("Error while rollback inserting ILM_METADATA_HISTORY into Db : "+e);
				e1.printStackTrace();
			}
			throw new Exception(e);
		} catch (Exception e) {
			Logger.error("Error while inserting ILM_METADATA_HISTORY into Db : "+e);
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
			DatabaseConnection.close(stmt);
			DatabaseConnection.close(preparedStatement);
			DatabaseConnection.close(connectionObj); 
			Logger.info("Connection closed");
		}
		Logger.info("Exiting insertAll");
		
	}
	
	public static List<MetadataHistory> getMetdataHistoryForImpactedData(Integer cycleId) throws Exception{
		
/*		select h.* from ILM_METADATA m
		inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code)
		right join ILM_METADATA_HISTORY h on m.guid = h.guid
		where r.cycle_Id = 301 and m.status != 'DELETED'
		order by h.guid*/
		
		/*SqlQuery sqlQuery = Ebean.createSqlQuery("select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE, h.COUNTRY_CODE, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.EBR_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" and m.status != 'DELETED' "
				+ "order by h.guid");
		sqlQuery.setMaxRows(5000000);
	    List<SqlRow> sqlRows = sqlQuery.findList();

		Logger.info("No of history records "+sqlRows.size());
		
		
		for(SqlRow a : sqlRows){
			MetadataHistory m = new MetadataHistory();
			m.setId(a.getString("ID"));
			m.setCycleId(a.getInteger("CYCLE_ID"));
			m.setGuid(a.getString("GUID"));
			m.setProjectId(a.getString("PROJECT_ID"));
			m.setAit(a.getString("AIT"));
			m.setRecordCode(a.getString("RECORD_CODE"));
			m.setCountry(a.getString("COUNTRY_CODE"));
			m.setRetentionEndPrev(a.getLong("RETENTION_END_DATE_PREV"));
			m.setRetentionEndNew(a.getLong("RETENTION_END_DATE_NEW"));
			m.setStorageUriPrev(a.getString("STORAGE_URI_PREV"));
			m.setStorageUriNew(a.getString("STORAGE_URI_NEW"));
			m.setRetentionTypePrev(a.getString("RETENTION_TYPE_PREV"));
			m.setRetentionTypeNew(a.getString("RETENTION_TYPE_NEW"));
			m.setDbUpdateFlag(a.getString("DB_UPDATE_FLAG"));
			m.setEbrComment(a.getString("EBR_COMMENT"));
			m.setChangeType(a.getString("CHANGE_TYPE"));
			m.setIndexFileUriPrev(a.getString("INDEX_FILE_URI_PREV"));
			m.setIndexFileUriNew(a.getString("INDEX_FILE_URI_NEW"));
			hist.add(m);
		}
		
		return hist;*/
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
					+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
					+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
					+ "from ILM_METADATA m "
					+ "inner join RETENTION_UPDATES r on (m.record_code = r.record_code and m.country = r.country_code) "
					+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
					+ "where r.cycle_Id = "+cycleId+" "
					+ "and m.status != 'DELETED' "
					+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
		return metadataList;
	}
	
	public static List<MetadataHistory> getMetdataHistoryImpactedDataForRCA(Integer cycleId) throws Exception{
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RCA_UPDATES r on (m.project_id = r.project_id) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
	List<MetadataHistory> metadataList = executeQuery(query);
	return metadataList;
	}
	
	public static List<MetadataHistory> listByAitAndProjectidForRCA(Integer cycleId, String ait, String projectid)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RCA_UPDATES r on (m.project_id = r.project_id) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByAitForRCA(Integer cycleId,String ait)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RCA_UPDATES r on (m.project_id = r.project_id) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.ait = '"+ait+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<MetadataHistory> listByProjectIdForRCA(Integer cycleId, String projectid)  throws Exception{
		
		String query = "select h.ID, h.CYCLE_ID, h.GUID, h.PROJECT_ID, h.AIT, h.RECORD_CODE_NEW, h.COUNTRY_CODE_NEW, "
				+ "h.RETENTION_END_DATE_PREV, h.RETENTION_END_DATE_NEW, h.STORAGE_URI_PREV, h.STORAGE_URI_NEW, h.RETENTION_TYPE_PREV, h.RETENTION_TYPE_NEW, "
				+ "h.DB_UPDATE_FLAG, h.UPDATE_COMMENT, h.CHANGE_TYPE, h.INDEX_FILE_URI_PREV, h.INDEX_FILE_URI_NEW "
				+ "from ILM_METADATA m "
				+ "inner join RCA_UPDATES r on (m.project_id = r.project_id) "
				+ "right join ILM_METADATA_HISTORY h on m.guid = h.guid "
				+ "where r.cycle_Id = "+cycleId+" "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.status != 'DELETED' "
				+ "order by h.guid";
		List<MetadataHistory> metadataList = executeQuery(query);
        return metadataList;
	}
	
	private static List<MetadataHistory> executeQuery(String query) throws Exception {
		List<MetadataHistory> hist = new ArrayList<MetadataHistory>();
		
		Connection connectionObj = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			Logger.info("Getting DB Connection");	
			connectionObj = DatabaseConnection.getUDASDBConnection() ;
			stmt = connectionObj.createStatement();
			rs = stmt.executeQuery(query);
			
			while(rs.next()){
				MetadataHistory m = new MetadataHistory();
				m.setId(rs.getString("ID"));
				m.setCycleId(rs.getInt("CYCLE_ID"));
				m.setGuid(rs.getString("GUID"));
				m.setProjectId(rs.getString("PROJECT_ID"));
				m.setAit(rs.getString("AIT"));
				m.setRecordCodeNew(rs.getString("RECORD_CODE_NEW"));
				m.setCountryNew(rs.getString("COUNTRY_CODE_NEW"));
				m.setRetentionEndPrev(rs.getLong("RETENTION_END_DATE_PREV"));
				if(m.getRetentionEndPrev() == 0){
					m.setRetentionEndPrev(null);
				}
				m.setRetentionEndNew(rs.getLong("RETENTION_END_DATE_NEW"));
				if(m.getRetentionEndNew() == 0){
					m.setRetentionEndNew(null);
				}
				m.setStorageUriPrev(rs.getString("STORAGE_URI_PREV"));
				m.setStorageUriNew(rs.getString("STORAGE_URI_NEW"));
				m.setRetentionTypePrev(rs.getString("RETENTION_TYPE_PREV"));
				m.setRetentionTypeNew(rs.getString("RETENTION_TYPE_NEW"));
				m.setDbUpdateFlag(rs.getString("DB_UPDATE_FLAG"));
				m.setEbrComment(rs.getString("UPDATE_COMMENT"));
				m.setChangeType(rs.getString("CHANGE_TYPE"));
				m.setIndexFileUriPrev(rs.getString("INDEX_FILE_URI_PREV"));
				m.setIndexFileUriNew(rs.getString("INDEX_FILE_URI_NEW"));
				hist.add(m);
			}

		} catch (SQLException e) {
			Logger.error("Error while getting getMetdataHistoryForImpactedData : "+e);
			e.printStackTrace();
			throw new Exception(e);
		} catch (Exception e) {
			Logger.error("Error while getting getMetdataHistoryForImpactedData : "+e);
			e.printStackTrace();
			throw new Exception(e);
		}finally{
			DatabaseConnection.close(rs);
			DatabaseConnection.close(stmt);
			DatabaseConnection.close(connectionObj); 
			Logger.info("Connection closed");
		}
		
		return hist;
	}
	
	public static int updateStorageUriStatus(String GUID, String storageUri, String status)
	{
		
		String s = "UPDATE ILM_METADATA_HISTORY set STORAGE_URI_PREV_STATUS = :status where GUID = :GUID and STORAGE_URI_PREV = :storageUri";
		SqlUpdate update = Ebean.createSqlUpdate(s);
		update.setParameter("status", status);
		update.setParameter("GUID", GUID);
		update.setParameter("storageUri", storageUri);
		int modifiedCount = Ebean.execute(update);
		return modifiedCount;
	}
	
	public static Boolean checkUpdateCommentsIfAny(String guid) {
		List<String> listUpdateComment =  new ArrayList<String>();
		List<String> strings = new ArrayList<String>();
		Collections.addAll(listUpdateComment,Constant.DISABLED,Constant.EBR_TO_EBR_EXT,Constant.EBR_TO_EBR_SHO,Constant.EBR_TO_EBR_NO_CHANGE,Constant.PERMOFF,Constant.UNTRIGGERED);
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("guid", guid).in("ebrComment", listUpdateComment).findList();
		
		if(metadataList.size()>0) return true;
        
		return false;
	}
	public static Boolean checkNullRetentionEndDate(String guid) {
		List<MetadataHistory> metadataList = MetadataHistory.find.where().eq("guid", guid).isNull("retentionEndPrev")
        .findList();
		
		if(metadataList.size()>0) return true;
        
		return false;
	}
	
	public static Long getMaxRetentionEndDate(String guid) {
		String s = "select max(RETENTION_END_DATE_PREV) from ILM_METADATA_HISTORY where GUID = :GUID";
		SqlQuery sqlQuery = Ebean.createSqlQuery(s);
		sqlQuery.setParameter("GUID", guid);
		Long maxRetention=0L;
		List<SqlRow> result = sqlQuery.findList();
	    if (result.size() > 0) {     
	    	maxRetention = result.get(0).getLong("max(RETENTION_END_DATE_PREV)");
	    }
	
		return maxRetention;
	}
}
