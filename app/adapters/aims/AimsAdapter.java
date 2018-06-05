package adapters.aims;

import javax.sql.DataSource;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import utilities.DataConnection;
import adapters.AdapterException;
import adapters.aims.RecordCode.TimeUnit;

public final class AimsAdapter {

	private static final Logger log = Logger.getLogger(AimsAdapter.class.getName());

	private static class Holder {
		private static AimsAdapter INSTANCE = null;
		public static AimsAdapter getInstance() {
			if (INSTANCE == null) {
				try {
					System.out.println("creating aimsadtaperinstance");
					INSTANCE = new AimsAdapter(DataConnection.getAimsDataSource());
				} catch (Exception e) {}
			}
			return INSTANCE;
		}
	}

	private DataSource dataSource = null;	

	private AimsAdapter(DataSource dataSource) {
		System.out.println("setting datasource");
		this.dataSource = dataSource;
	}
	public static AimsAdapter getInstance() {
		return Holder.getInstance();
	}

	public AimsProject getProjectDetails(String projectId) throws Exception {

		System.out.println("Entered into AIMSAdapter -  getProjectDetails Method");

		AimsCache aimsCache = new AimsCache();
		AimsProject cachedProject = (AimsProject)aimsCache.getProject(projectId);

		java.util.Date date= new java.util.Date();

		if(cachedProject!=null){
			long diff = (date.getTime() - cachedProject.getCacheTimestamp().getTime());
			long diffMinutes = diff / (60 * 1000) % 60;
			System.out.println("diffMinutes "+diffMinutes);
			if(diffMinutes<5){
				System.out.println("Recent Object- Getting from Cache");

				return cachedProject;
			}
		}


		Connection connection = null;
		PreparedStatement pst = null;

		try{
			connection = dataSource.getConnection();
		}catch(Exception e){
			if(cachedProject!=null){
				System.out.println("Cannot connect to AIMS DB -  getting project details from cache.");
				return cachedProject;
			}

			log.severe("Error in retireving AIMS Project Details" + e);
		}

		try {
			System.out.println("getting project details from aims db");
			AimsProject project = new AimsProject();
			String strQuery = "select a.Project_Name, a.ait_id from Archive_Project a where a.DLMS_ProjectID= ?";
			pst = connection.prepareStatement(strQuery);
			pst.setString(1, projectId);
			ResultSet rs = pst.executeQuery();
			if (rs.next()) {
				project.setId(projectId);
				project.setName(rs.getString(1));
				String aitId = rs.getString(2);
				project.ait = getAit(aitId, connection, false);
				project.recordCode = getRecordCode(projectId);



				aimsCache.addProject(project);

				System.out.println("Archive Project details for "+projectId +"is"+ project.toString());
				System.out.println("Refreshing Cache");

				return project;
			}
		} catch (SQLException e) {
			log.severe("Error in retireving AIMS Project Details" + e);
		} finally {
			DataConnection.closeStatement(pst);
			DataConnection.closeConnection(connection);
			log.fine("Exiting from AIMSAdapter -  getProjectDetails Method");
		}
		return null;
	}

	public static RecordCode getRecordCode(String code, String countryCode, long retentionPeriod, TimeUnit timeUnit, String recordName, String usOfficialEventType) throws AdapterException {
		RecordCode rCode=null;
		try{
			if(usOfficialEventType != null && !"".equals(usOfficialEventType)){
				rCode= new RecordCode(code,countryCode,retentionPeriod,timeUnit,recordName,usOfficialEventType);
			}else{
				rCode= new RecordCode(code,countryCode,retentionPeriod,timeUnit,recordName);
			}
			
		}
		catch(Exception e){
			log.severe("Error in generating Record Code" + e);
			throw new AdapterException("C~"+e.getMessage());
		}
		return rCode;
	}

	public RecordCode getRecordCode(String projectId) throws AdapterException {
		System.out.println("Entered into AIMSAdapter -  getRecordCode Method");

		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement pst = null;
		StringBuffer strQuery = new StringBuffer();
		RecordCode rCodeObj = null;

		try {
			connection = dataSource.getConnection();
			/**
			 * Record Code table will be populated based on the Feed from the
			 * GRM Team and some time record code might be deleted. So we are
			 * adding the Left Join as that Record Code might be mapped in the
			 * AIMS GRM_Metadata_Details already
			 */
			//strQuery.append("select RC.Record_Code, RC.Country_Code as country_code,RC.US_Official_Retention, RC.DispositionTimeUnit,RC.Record_Full_Name,RC.US_Official_Event_type from GRM_Metadata_Details AS GRM  ");
			strQuery.append("select RC.Record_Code, GRM.Rec_Country_of_Origin as country_code,RC.US_Official_Retention, RC.DispositionTimeUnit,RC.Record_Full_Name,RC.US_Official_Event_type from GRM_Metadata_Details AS GRM  ");
			strQuery.append(" LEFT JOIN Record_Codes AS RC  on GRM.Record_Code=RC.Record_Code and GRM.Country_Code=RC.Country_Code  ");
			strQuery.append("where GRM.ProjectID = ?");

			pst = connection.prepareStatement(strQuery.toString());
			pst.setInt(1, Integer.parseInt(projectId));
			rs = pst.executeQuery();
			while (rs.next()) {
				String rCode=rs.getString(1);
				String CountryCode=rs.getString(2);
				if (CountryCode == null || CountryCode.length() ==0) CountryCode = "US";
				long retention=0L;
				TimeUnit timeUnit = TimeUnit.DAY; //Default
				if(rs.getString(6).equalsIgnoreCase("permanent")){
					retention=-1L;
				}else{
					retention=(long)Double.parseDouble(rs.getString(3));
					if(rs.getString(4).equalsIgnoreCase("Year(s)")){
					timeUnit=TimeUnit.YEAR;
					}
					else if(rs.getString(4).equalsIgnoreCase("Month(s)")){
						timeUnit=TimeUnit.MONTH;
					}
					else if(rs.getString(4).equalsIgnoreCase("Day(s)")){
						timeUnit=TimeUnit.DAY;
					}
					
				}
				rCodeObj=new RecordCode(rCode,CountryCode,retention,timeUnit,rs.getString(5),rs.getString(6));
			}

			System.out.println("Record Code details for " + projectId
					+ "is" + rCodeObj.toString());

		} catch (SQLException e) {
			log.severe("Error in retireving AIMS Project Record Code Details" + e);
			throw new AdapterException("C~"+e.getMessage());
		}catch(Exception ex){
			log.severe("Error in retireving AIMS Project Record Code Details" + ex);
			throw new AdapterException("C~"+ex.getMessage());
		}

		finally {
			DataConnection.closeStatement(pst);
			DataConnection.closeConnection(connection);
		}
		log.fine("Exiting from AIMSAdapter -  getRecordCode Method");

		return rCodeObj;

	}

	public Ait getAit(String aitId) throws Exception {

		return getAit(aitId, null, true);
	}

	private Ait getAit(String aitId, Connection conn, boolean close) throws Exception {

		System.out.println("Entered into AIMSAdapter -  getAit Method");

		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			String aitQuery = "select AIT_Name from AIT where AIT_ID=?";

			if (conn == null) connection = dataSource.getConnection();
			else connection = conn;

			pstmt = connection.prepareStatement(aitQuery);
			pstmt.setString(1,aitId);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				Ait ait = new Ait(aitId, rset.getString("AIT_Name"));

				System.out.println("AIT details for is "+ ait.toString());
				return ait;
			}
		} catch (SQLException e) {
			log.severe("Error in retireving AIMS Project Details" + e);

		} finally {
			DataConnection.closeStatement(pstmt);
			if (close) DataConnection.closeConnection(connection);
			log.fine("Exiting from AIMSAdapter -  getAit Method");
		}
		return null;
	}



	public Boolean authorizeUser(String nbkid,String roleID,String projectID) throws AdapterException {
		System.out.println("In Aims controller "+roleID+"  "+nbkid+"   "+projectID);
		System.out.println("In authorizeUser()");

		if (nbkid == null || nbkid.length() == 0) return Boolean.FALSE;
		if (roleID == null || roleID.length() ==0) return Boolean.FALSE;
		if (projectID == null || projectID.length() ==0) return Boolean.FALSE;

		if (isAuthorized(nbkid, roleID, projectID)) return Boolean.TRUE; // use data in getDataSecurityDetails

		PreparedStatement statement =null;
		Connection con = null;
		Boolean result = Boolean.FALSE;	

		try{
			con = dataSource.getConnection();

			String [] roles = null;
			int idx = roleID.indexOf(",");
			if (idx != -1) roles = roleID.split(",");
			else {
				roles = new String[1];
				roles[0] = roleID;
			}

			StringBuilder strQuery = new StringBuilder("select * from RoleEmployeeProjectMap where ProjectID = ? and EmployeeID = ? and RoleID in (");
			for(String role : roles) {
				strQuery.append("?").append(",");
			}
			strQuery.append("0)");

			statement = con.prepareStatement(strQuery.toString());

			statement.setString(1, projectID);

			statement.setString(2, nbkid);

			int count = 0;
			for(String role : roles) {
				statement.setString(count+3, roles[count]);
				count ++;
			}

			ResultSet rs = statement.executeQuery();

			if(rs.next()){
				result = Boolean.TRUE;
			}

		}catch(Exception e){
			log.severe("Error in authorizeUser() "+ e);
			throw new AdapterException(e.getMessage());
		}finally{
			DataConnection.closeStatement(statement);
			DataConnection.closeConnection(con);
		}

		return result;
	}

	/***
	 * Code Added by Satish to get the User Info
	 * @param nbkID
	 * @return
	 * @throws AdapterException
	 */
	public User getUserInfo(String nbkID) throws AdapterException {	
		log.warning("Entered into AIMSAdapter -  getUserInfo Method");
		Connection connection = null;
		PreparedStatement pst = null;
		ResultSet resultSet = null;
		StringBuffer strQuery = new StringBuffer();
		User userObj = null;
		AimsProject projectsObj = null;
		List<AimsProject> projects = new ArrayList<AimsProject>();

		try{
			connection = dataSource.getConnection();
			strQuery.append(" SELECT DISTINCT AP.DLMS_ProjectID , USERS.Last_Name ,  USERS.First_Name,   ");
			strQuery.append(" AP.Project_Name FROM Archive_Project AS  AP   ");		
			strQuery.append(" INNER JOIN RoleEmployeeProjectMap  AS RM  ON AP.DLMS_ProjectID=RM.ProjectID JOIN Users AS Users ON RM.EmployeeID = Users.NBKID ");		
			strQuery.append(" WHERE AP.isDeleted=0 AND RM.EmployeeID= ?  ORDER BY AP.DLMS_ProjectID DESC ");			
			pst = connection.prepareStatement(strQuery.toString());
			pst.setString(1, nbkID);
			resultSet = pst.executeQuery();	

			while (resultSet.next()){//Iterating the Result set and populating the AIMS User information with Project Details 
				if (userObj == null) {
					userObj =  new User();
					userObj.setUserName(nbkID);
					userObj.setLastName(resultSet.getString(2));
					userObj.setFirstName(resultSet.getString(3));
				}
				projectsObj =  new AimsProject();
				projectsObj.setId(resultSet.getString(1));
				projectsObj.setName(resultSet.getString(4));			

				projects.add(projectsObj);//Add the Project Information to User Object			
			}//End of While

			if (projects != null && projects.size() > 0){//Check the Project List Size
				userObj.setProjects(projects);
			}		

		}catch (SQLException e) {		
			log.severe("Error in retireving AIMS User Info Details" + e);
			throw new AdapterException("C~"+e.getMessage());
		}catch (Exception e) {		
			log.severe("Error in retireving AIMS User Info Details" + e);
			throw new AdapterException("C~"+e.getMessage());
		} finally {
			DataConnection.closeStatement(pst);
			DataConnection.closeConnection(connection);
		}
		log.warning("Exiting from AIMSAdapter -  getUserInfo Method");
		return userObj;
	}

	public String getDataSecurityDetails(String projectid) throws AdapterException {
		if (projectid == null || projectid.length() == 0) return "";

		String acl = "";
		Connection connection = null;
		PreparedStatement pst = null;

		try {
			connection = dataSource.getConnection();
			StringBuffer strQuery = new StringBuffer();
			strQuery.append("select data_security_details from archive_access_details where projectid = ?");
			pst = connection.prepareStatement(strQuery.toString());
			pst.setString(1, projectid);
			ResultSet resultSet = pst.executeQuery();
			if (resultSet.next()) {
				acl = resultSet.getString(1);
				if (acl != null) return acl;
			}
		} catch (Exception e) {
			System.out.println("throwing adapter exception");
			throw new AdapterException(e.getMessage());
		} finally {
			DataConnection.closeStatement(pst);
			DataConnection.closeConnection(connection);
		}
		return acl;
	}

	private boolean isAuthorized(String nbkid,String roleID,String projectID) throws AdapterException {
		if (roleID == null || roleID.length() ==0) return false;
		if (nbkid == null || nbkid.length() ==0) return false;

		String acl = getDataSecurityDetails(projectID).toUpperCase();
		if (acl == null || acl.length() ==0) return false;

		if (acl.indexOf(nbkid.toUpperCase()) == -1) return false;

		String[] roles = null;
		int idx = roleID.indexOf(",");
		if (idx != -1) roles = roleID.split(",");
		else {
			roles = new String[1];
			roles[0] = roleID;
		}

		String[] groups = null;
		idx = acl.indexOf(";");
		if (idx != -1) groups = acl.split(";");
		else {
			groups = new String[1];
			groups[0] = acl;
		}

		for(String group : groups) {
			for (String role : roles) {
				if (isUserInRole(nbkid, role, group)) return true;
			}
		}
		return false;
	}

	private boolean isUserInRole(String nbkid, String role, String group) {
		if (group.indexOf(":") == -1) return false;

		String[] roleUsers = group.split(":");
		if (!roleUsers[0].equals(role)) return false;

		String[] users = null;
		int idx = roleUsers[1].indexOf(",");
		if (idx != -1) users = roleUsers[1].split(",");
		else {
			users = new String[1];
			users[0] = roleUsers[1];
		}

		for(String user : users) {
			String _user = user.trim();
			if (_user.equalsIgnoreCase(nbkid)) return true;
		}
		return false;
	}
	
	public int[] updateUDASLudsOnAIMSDB(List<UdasLudEntity> udasLuds) {
		
		int[] resultList = null;
		
		if(udasLuds == null || udasLuds.isEmpty()) {
			return resultList;
		}
		
		Connection connection = null;
		PreparedStatement preparedStatement = null;

		try{
			connection = dataSource.getConnection();
			preparedStatement = connection.prepareStatement(
					AIMSConstants.UDAS_LUD_UPDATE_QUERY);
			
			connection.setAutoCommit(false);
			
			for(UdasLudEntity ludEntity : udasLuds) {
				preparedStatement.setDate(1, ludEntity.getLud());
				preparedStatement.setString(2, ludEntity.getUser_id());
				preparedStatement.setString(3, ludEntity.getRole().toString());
				
				preparedStatement.addBatch();
			}
			
			resultList = preparedStatement.executeBatch();
			
			connection.commit();
			
		}catch(Exception e){
			log.severe("Error in updating UDAS LUDs on AIMS " + e);
			if(connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					log.severe("Error while rolling back updates on AIMS " + e);
					e1.printStackTrace();
				}
			}
		} finally {
			DataConnection.closeStatement(preparedStatement);
			DataConnection.closeConnection(connection);
		}
		
		return resultList;
	}
	
}