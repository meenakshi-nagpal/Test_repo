package models.storageapp;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


import adapters.aims.AIMSRoleMapping;
import adapters.aims.UdasLudEntity;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlQuery;
import com.avaje.ebean.SqlRow;

import play.db.ebean.Model;
import utilities.DateUtil;

@Entity
@Table(name="ILM_ACCESS_DATA")
public class AccessData extends Model{


	private static final long serialVersionUID = -4420963753350334122L;

	@Id
	private String id = "";

	@Column(name="USER_ID")
	private String userId = "";

	@Column(name="PROJECT_ID")
	private String projectId = "";

	@Column(name="IS_SUCCESSFULL")
	private Integer isSuccessfull = 0;

	@Column(name="ACCESS_TIMESTAMP")
	private Long accessTimeStamp = 0L;

	@Column(name="ROLE_ID")
	private String role = "";

	@Column(name="URI")
	private String uri = "";

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = (userId != null ? userId.toUpperCase() : userId);
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public Integer getIsSuccessfull() {
		return isSuccessfull;
	}

	public void setIsSuccessfull(Integer isSuccessfull) {
		this.isSuccessfull = isSuccessfull;
	}

	public Long getAccessTimeStamp() {
		return accessTimeStamp;
	}

	public void setAccessTimeStamp(Long accessTimeStamp) {
		this.accessTimeStamp = accessTimeStamp;
	}
	
	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public Boolean authorizeFromAccessData(String nbkid, String role, String projectID) throws 
	Exception{

		Boolean result  = false;

		Finder<Long, AccessData> find = new Finder<Long, AccessData>(Long.class,
				AccessData.class);
		List<AccessData> accessDataList = find.where().eq(
				"USER_ID", nbkid.toUpperCase()).eq("PROJECT_ID",projectID).eq(
						"ROLE_ID",role).eq("IS_SUCCESSFULL", 1).findList();

		System.out.print("Size of list is-"+accessDataList.size());

		if(accessDataList.size()>=1){
			result = true;
		}

		return result;
	}
	
	
	public List<UdasLudEntity> retrieveLUDsFromDateToToDate(Date fromDate, Date toDate) {
		List<UdasLudEntity> udasLudEntities = new LinkedList<>();
		
		if(fromDate == null || toDate == null) {
			return udasLudEntities;
		}
		
		String fromDateString = DateUtil.formatDateYYYY_MM_DD(fromDate);
		String toDateString = DateUtil.formatDateYYYY_MM_DD(toDate);
		
		SqlQuery sqlQuery = Ebean.createSqlQuery(ludQuery);
		
		sqlQuery.setParameter(1, fromDateString);
		sqlQuery.setParameter(2, toDateString);
		
		List<SqlRow> sqlRows = sqlQuery.findList();
		
		for(SqlRow row : sqlRows) {
			UdasLudEntity ludEntity = new UdasLudEntity();
			
			ludEntity.setUser_id(row.getString("USER_ID"));
			ludEntity.setRole(
					AIMSRoleMapping.getInstance(
							Integer.valueOf(
									row.getString("ROLE_ID"))));
			ludEntity.setLud(new java.sql.Date(
					DateUtil.parseDateYYYY_MM_DD(
							row.getString(
									"LUD")).getTime()));
			
			udasLudEntities.add(ludEntity);
		}

		return udasLudEntities;
	}

	private static final String ludQuery = "SELECT USER_ID, " +
			"  ROLE_ID, " +
			"  TO_CHAR ((DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000) * MAX(ACCESS_TIMESTAMP)), 'yyyy-MM-dd') LUD " +
			"FROM ILM_ACCESS_DATA " +
			"WHERE (DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000) * ACCESS_TIMESTAMP) > to_date (?, 'yyyy-MM-dd') " +
			"AND (  DATE '1970-01-01' + ( 1 / 24 / 60 / 60 / 1000) * ACCESS_TIMESTAMP) < to_date (?, 'yyyy-MM-dd') " +
			"AND IS_SUCCESSFULL                                                        = 1 " +
			"GROUP BY USER_ID, " +
			"  ROLE_ID " +
			"ORDER BY user_id";

}