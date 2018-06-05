package models.storageapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import jobs.util.DatabaseConnection;
import play.Logger;
import play.cache.Cache;
import play.db.ebean.Model;
import utilities.Constant;
import utilities.Utility;
import adapters.cas.CenteraClip;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Page;
import com.avaje.ebean.SqlQuery;
import com.avaje.ebean.SqlRow;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@Table(name="ILM_METADATA")
public class Metadata extends Model  {

		private static final long serialVersionUID = 7969000271573056192L;
		
		@Id
		private String id = "";					//primary key
		
		@Column(name="GUID")
		private String guid ="";			//storage app generated unique id AKA GUID	
		
		@Column(name="project_Id")
		private String projectId = "";			//identifier from the source - project id

		@Column(name="ait")
		private String ait = "";

		@Column(name = "file_name", columnDefinition = "TEXT")	
		private String fileName ="";	
	
		@Column(name = "file_name_case", columnDefinition = "TEXT")	
		private String fileNameCase ="";	

		@Column(name="file_size")
		private Long fileSize = 0L;
		
		@Column(name="ingestion_start")
		private Long ingestionStart = 0L;
		
		@Column(name="ingestion_end")
		private Long ingestionEnd = 0L;  // the time centera returns after uploading 
		
		@Column(name="ingestion_user")
		private String ingestionUser = "";
		
		@Column(name="record_code")
		private String recordCode = "";	//from source - define by admin		
		
		@Column(name="country")
		private String country = "US";		
		
		@Column(name="retention_start")
		private Long retentionStart = 0L;   // start of retention, and for retention period it will be same as storage time stamp
		
		@Column(name="retention_end")
		private Long retentionEnd = 0L;
		
		@Column(name="legal_hold")
		private char legalHold = 'N';
		
		@Column(name="status")
		private String status = "PENDING";
		
		@Column(name="modification_timestamp")
		private Long modificationTimestamp = 0L;
		
		@Column(name="modified_by")
		private String modifiedBy = "";		
		
		@Column(name="uri_scheme_authority")
		private String uriSchemeAuthority = "";	//storage URI Scheme and Authority
		
		@Column(name="storage_uri")
		private String storageUri = "";			//storage URI Path
		
		@Column(name="source_server")
		private String sourceServer = "";

		@Column(name="source_namespace")
		private String sourceNamespace = "";

		@Column(name="user_agent")
		private String userAgent = "";
		
		@Column(name="extended_metadata", columnDefinition = "TEXT")
		private String extendedMetadata = "";

		@Column(name="metadata_url", columnDefinition = "TEXT")
		private String metadataUrl = "";

		@Column(name="metadata_size")
		private Long metadataSize = 0L;

		@Column(name="metadata_schema", columnDefinition = "TEXT")
		private String metadataSchema = "";

		@Column(name="location_url", columnDefinition = "TEXT")
		private String locationUrl = "";

		@Column(name="extended_status", columnDefinition = "TEXT")
		private String extendedStatus = "";
		
		@Transient
		@JsonIgnore
		private Integer aimsRegistrationStatus;
		
		@Column(name="ARCHIVE_STORAGE_ID")
		@JsonIgnore
		private Integer archiveStorageId;

		@Column(name="INDEX_FILE_STORAGE_ID")
		@JsonIgnore
		private Integer indexFileStorageId;
		
		@Column(name="IS_COMPRESSED")
		@JsonIgnore
		private char isCompressed = 'N';
		
		@Column(name="pre_compressed_size")
		@JsonIgnore
		private Long preCompressedSize = 0L;

		@Column(name="EVENT_BASED")
		private String eventBased="";
		
	
/*		@OneToOne(optional = true, mappedBy = "udasMetadata")
		private AIMSRegistration aimsRegistration;
		
		public AIMSRegistration getAimsRegistration() {
			return aimsRegistration;
		}

		public void setAimsRegistration(AIMSRegistration aimsRegistration) {
			this.aimsRegistration = aimsRegistration;
		}*/
		

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder();

			str.append("{")
				.append("\"").append("id").append("\"").append(":").append(id)
				.append(",")
				.append("\"").append("guid").append("\"").append(":").append("\"").append(guid).append("\"")
				.append(",")
				.append("\"").append("projectId").append("\"").append(":").append("\"").append(projectId).append("\"")
				.append(",")
				.append("\"").append("storageUri").append("\"").append(":").append("\"").append(storageUri).append("\"")
				.append(",")
				.append("\"").append("extendedStatus").append("\"").append(":").append("\"").append(extendedStatus).append("\"")
				.append("\"").append("archiveStorageId").append("\"").append(":").append("\"").append(archiveStorageId).append("\"")
				.append("}");

			return str.toString();
		}

		private String toCase(String value) {
			return (value != null ? value.toUpperCase() : value);
		}

		protected String getId(){
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}

		public String getAit() {
			return ait;
		}
		public void setAit(String ait) {
			this.ait = ait;
		}

		public String getLocationUrl() {
			return locationUrl;
		}
		public void setLocationUrl(String locationUrl) {
			this.locationUrl = locationUrl;
		}

		protected String getExtendedMetadata() {
			return extendedMetadata;
		}
		
		public void setExtendedMetadata(String extendedMetadata) {
			this.extendedMetadata = (extendedMetadata != null ? extendedMetadata.trim() : extendedMetadata);
		}

		public String getMetadataUrl() {
			return metadataUrl;
		}
		public void setMetadataUrl(String metadataUrl) {
			this.metadataUrl = metadataUrl;
		}

		public Long getMetadataSize() {
			return metadataSize;
		}
		public void setMetadataSize(Long metadataSize) {
			this.metadataSize = metadataSize;
		}

		public String getMetadataSchema() {
			return metadataSchema;
		}
		public void setMetadataSchema(String metadataSchema) {
			this.metadataSchema = metadataSchema;
		}

		public String getUserAgent() {
			return userAgent;
		}
		public void setUserAgent(String userAgent) {
			this.userAgent = userAgent;
		}

		public String getSourceServer() {
			return sourceServer;
		}
		public void setSourceServer(String sourceServer) {
			this.sourceServer = sourceServer;
		}

		public String getSourceNamespace() {
			return sourceNamespace;
		}
		public void setSourceNamespace(String sourceNamespace) {
			this.sourceNamespace = sourceNamespace;
		}
		
		public String getProjectId() {
			return projectId;
		}
		public void setProjectId(String projectId) {
			this.projectId = projectId;
		}

		public String getRecordCode() {
			return recordCode;
		}
		public void setRecordCode(String recordCode) {
			this.recordCode = toCase(recordCode);
		}
		public String getCountry() {
			return country;
		}
		public void setCountry(String country) {
			this.country = toCase(country);
		}
		protected String getStorageUri() {
			return storageUri;
		}
		public void setStorageUri(String storageUri) {
			this.storageUri = storageUri;
		}
		public String getGuid() {
			return guid;
		}
		public void setGuid(String guid) {
			this.guid = guid;
		}
		protected String getUriSchemeAuthority() {
			return uriSchemeAuthority;
		}
		public void setUriSchemeAuthority(String uriSchemeAuthority) {
			this.uriSchemeAuthority = uriSchemeAuthority;
		}

		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
			setFileNameCase(fileName);
		}
		protected String getFileNameCase() {
			return fileNameCase;
		}
		public void setFileNameCase(String fileNameCase) {
				this.fileNameCase = toCase(fileNameCase);
		}
		public Long getFileSize() {
			return fileSize;
		}
		public void setFileSize(Long fileSize) {
			this.fileSize = fileSize;
		}

		public Long getIngestionStart() {
			return ingestionStart;
		}
		public void setIngestionStart(Long ingestionStart) {
			this.ingestionStart = ingestionStart;
		}

		public Long getIngestionEnd() {
			return ingestionEnd;
		}
		public void setIngestionEnd(Long ingestionEnd) {
			this.ingestionEnd = ingestionEnd;
		}
		public String getIngestionUser() {
			return ingestionUser;
		}
		public void setIngestionUser(String ingestionUser) {
			this.ingestionUser = toCase(ingestionUser);
		}
		public Long getRetentionStart() {
			return retentionStart;
		}
		public void setRetentionStart(Long retentionStart) {
			this.retentionStart = retentionStart;
		}
		public Long getRetentionEnd() {
			return retentionEnd;
		}
		public void setRetentionEnd(Long retentionEndTime) {
			this.retentionEnd = retentionEndTime;
		}

		public String getStatus() {
			return status;
		}
		public void setStatus(String status) {
			this.status = toCase(status);
		}
		public String getExtendedStatus() {
			return extendedStatus;
		}
		public void setExtendedStatus(String extendedStatus) {
			this.extendedStatus = extendedStatus;
		}
		public Long getModificationTimestamp() {
			return modificationTimestamp;
		}
		public void setModificationTimestamp(long modificationTimestamp) {
			this.modificationTimestamp = modificationTimestamp;
		}
		public String getModifiedBy() {
			return modifiedBy;
		}
		public void setModifiedBy(String modifiedBy) {
			this.modifiedBy = toCase(modifiedBy);
		} 
		
		public char getLegalHold() {
				return legalHold;
		}
		
		@JsonIgnore
		public Integer getAimsRegistrationStatus() {
			return aimsRegistrationStatus;
		}

		@JsonIgnore
		public void setAimsRegistrationStatus(Integer aimsRegistrationStatus) {
			this.aimsRegistrationStatus = aimsRegistrationStatus;
		}

		public char getIsCompressed() {
			return isCompressed;
		}

		public void setIsCompressed(char isCompressed) {
			this.isCompressed = isCompressed;
		}

		public Long getPreCompressedSize() {
			return preCompressedSize;
		}

		public void setPreCompressedSize(Long preCompressedSize) {
			this.preCompressedSize = preCompressedSize;
		}


		public String getEventBased() {
			return eventBased;
		}
		public void setEventBased(String eventBased) {
			this.eventBased = eventBased;
		}
		
		//DB operations
		public static Finder<String, Metadata> find = new Finder(String.class,
				Metadata.class);

		public static List<Metadata> all() {
			return find.all();
		}

		public static void create(Metadata data) {
			data.save();
		}

		public void delete(String id) {
			find.where().eq("Id", id).findUnique().delete();
		}
		
		
		/**
	     * @param archiveId reference to Guid
	     */
		public static void deleteByGuid(String guid, String projectid)
		{
			Metadata cmdt = (Metadata)find.where().eq("guid", guid).eq("projectId", projectid).findUnique();
			cmdt.setStatus("DELETED");
			cmdt.setModificationTimestamp(Utility.getCurrentTime());
			cmdt.save();
		}

		public static Metadata findByGuid(String guid, String projectid)
		{
			Metadata cmdt = (Metadata)find.where().eq("guid", guid).eq("projectId", projectid).findUnique();
			return cmdt;
		}
		
		public static Metadata findById(String id)
		{
			Metadata cmdt = (Metadata)find.ref(id);
			return cmdt;
		}
		public static Metadata findByStorageUri(String storageUri)
		{
			Metadata cmdt = (Metadata)find.where().eq("storageUri", storageUri).findUnique();
			return cmdt;
		}

		public static List<Metadata> findByEventBased(String eventBased){
			List<Metadata> cmdt = find.where().eq("eventBased", eventBased).findList();
			return cmdt;
		}
		/**
	     * @param clipId reference to Archive engine id
	     */
		public static Metadata update(Metadata data)
		{
			data.setModificationTimestamp(Utility.getCurrentTime());
			data.save();
			return data;
		}
		
		
		/**
	     * Return a page of ClipMetaData
	     *
	     * @param page Page to display
	     * @param pageSize Number of computers per page
	     * @param sortBy Computer property used for sorting
	     * @param order Sort order (either or asc or desc)
	     * @param filter Filter applied on the name column
	     */
	    public static Page<Metadata> page(int page, int pageSize, String sortBy, String order, String filter) {
	    	
	    	Page<Metadata> pages=  
	            find.where()
	            	.ilike("fileName", "%" + filter + "%")
	                .orderBy(sortBy + " " + order)
	                .findPagingList(pageSize)
	                .setFetchAhead(false)
	                .getPage(page);
	        	return pages;
	    }

	public static List<Metadata> list(final long offset, final long pagesize, String projectid) {
		List<Metadata> metadataList = Metadata.find.where().eq("project_id", projectid)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findPagingList((int)pagesize)
        .setFetchAhead(false).getPage((int)offset).getList();
        return metadataList;
	}
	
	public static List<Metadata> listByAit(Integer cycleId,String ait)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("ait", ait)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByProjectId(Integer cycleId, String projectid)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("projectid", projectid)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
		.findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.project_id = '"+projectid+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByRecordCode(Integer cycleId, String recordCode)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("recordCode", recordCode)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByAitAndProjectid(Integer cycleId, String ait, String projectid)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("ait", ait).eq("projectId", projectid)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByAitAndRecordCode(Integer cycleId, String ait, String recordCode)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("ait", ait).eq("recordCode", recordCode)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByProjectidAndRecordCode(Integer cycleId, String projectid,String recordCode)  throws Exception{
/*		List<Metadata> metadataList = Metadata.find.where().eq("projectId", projectid).eq("recordCode", recordCode)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.project_id = '"+projectid+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
		return metadataList;
	}
	
	public static List<Metadata> listByAitAndProjectidAndRecordCode(Integer cycleId, String ait, String projectid,String recordCode) throws Exception {
/*		List<Metadata> metadataList = Metadata.find.where().eq("ait", ait).eq("projectId", projectid).eq("recordCode", recordCode)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();*/
		
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and m.record_code = '"+recordCode+"' "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
		return metadataList;
	}
	
	public static List<Metadata> listByRecordCodes(List<String> recordCodes) {
		List<Metadata> metadataList = Metadata.find.where().in("recordCode", recordCodes)
		.ne("status", "DELETED")
		.orderBy("ingestion_start desc")
        .findList();
        return metadataList;
	}
	
	public static List<Metadata> listByAitAndProjectidForRCA(Integer cycleId, String ait, String projectid)  throws Exception{
	
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RCA_UPDATES r "
				+ "where m.project_id = r.project_id "
				+ "and m.project_id = '"+projectid+"' "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
	public static List<Metadata> listByAitForRCA(Integer cycleId,String ait)  throws Exception{
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RCA_UPDATES r "
				+ "where m.project_id = r.project_id "
				+ "and m.ait = '"+ait+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
		
	public static List<Metadata> listByProjectIdForRCA(Integer cycleId, String projectid)  throws Exception{
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RCA_UPDATES r "
				+ "where m.project_id = r.project_id "
				+ "and m.project_id = '"+projectid+"' "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> metadataList = executeQuery(query);
        return metadataList;
	}
	
/*	public static List<Metadata> listByRetentionUpdateRecordCodesAndCountryCode() {
		List<Metadata> metadataList = Metadata.find.where().eq("retentionUpdates.recordCode",recordCode).eq("retentionUpdates.countryCode",country).findList();
        return metadataList;
	}*/
	
/*	public static List<Metadata> findAIMSUnregisteredList(final int pagesize) {
		List<Metadata> metadatas = 
				Metadata.find.fetch("aimsRegistration").findPagingList(
						pagesize).getAsList();
		// TODO Query Revisit
		List<Metadata> metadatas2 = new LinkedList<Metadata>();
		for(Metadata metadata :metadatas) {
			if(metadata.aimsRegistration == null) {
				metadatas2.add(metadata);
				continue;
			}
			if(metadata.aimsRegistration.getStatus() != 
					Constant.AIMS_REGISTRATION_STATUS_AIMS_REGISTRATION_COMPLETE) {
				metadatas2.add(metadata);
				continue;
			}
		}
		
		return metadatas2;
	}*/

	public static Metadata newInstance(String id, String locationUrl, CenteraClip clip, String eventBased) {
		if (id == null || id.length() == 0) return null;
		if (locationUrl == null || locationUrl.length() ==0) return null;
		if (clip == null) return null;

        Metadata metadata = new Metadata();
        metadata.setId(id);
        metadata.setLocationUrl(locationUrl);

        metadata.setIngestionStart(clip.ingestionStart);
        metadata.setIngestionEnd(clip.ingestionEnd);
        metadata.setModificationTimestamp(clip.ingestionEnd);
        metadata.setAit(clip.ait);
        metadata.setProjectId(clip.projectid);  
        metadata.setCountry(clip.country);
        metadata.setFileName(clip.filename);
    	metadata.setIngestionUser(clip.creationUser);
    	metadata.setModifiedBy(clip.creationUser);
    	metadata.setGuid(clip.guid);
    	metadata.setFileSize(clip.size);
       	metadata.setRecordCode(clip.recordCode);
    	metadata.setRetentionStart(clip.getRetentionStart());
    	metadata.setRetentionEnd(clip.getRetentionEnd());
    	
    	metadata.setSourceServer(clip.sourceServer);
    	metadata.setUserAgent(clip.userAgent);
    	metadata.setSourceNamespace(clip.sourceNamespace);
        metadata.setStatus("ARCHIVED");
        metadata.setEventBased(eventBased);
        
        Integer StorageTypeId = Integer.valueOf(
        		(Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_TYPE));
		Logger.debug("MetadataController: Archive storageType: "+StorageTypeId);
		
		if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID)
		{
			metadata.setStorageUri(clip.clipid);//Centera clip id
			metadata.setUriSchemeAuthority("cas://" + clip.profile + "@" + clip.server); 
		}
		else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID)
		{
			metadata.setStorageUri(clip.guid);  //HCP used guid as file name
	    	metadata.setUriSchemeAuthority(null); //HCP Doesnt have storage uri
		}
		
        if(clip.getCompressionEnabled()!=null && clip.getCompressionEnabled().equalsIgnoreCase("true")){
        	metadata.setIsCompressed('Y');
        	metadata.setPreCompressedSize(clip.preCompressedSize);
        }else{
        	metadata.setIsCompressed('N');
        	metadata.setPreCompressedSize(null);
        }
        
        // Multiple CAS Support
        // Set Storage - CURRECT ACTIVE ID
      
       Integer CURRENT_WRITE_STORAGE_ID = Integer.valueOf((Integer)Cache.get(Constant.CURRENT_WRITE_STORAGE_KEY));
      
        
        metadata.setArchiveStorageId(CURRENT_WRITE_STORAGE_ID);
        
        //Set retention end to null if record code is infinite
        if(metadata.getRetentionEnd().compareTo(new Long(-1L))==0){
        	metadata.setRetentionEnd(null);
        }

        return metadata;
	}

    public void unidirectionalStatusChange(String fromStatus, String toStatus) throws Exception {
        fromStatus = toCase(fromStatus);
        toStatus = toCase(toStatus);

        if(fromStatus.equals(this.getStatus())) {
            this.setStatus(toStatus);
        }
        else {
            throw new Exception("Initial status did not match " + fromStatus);
        }
    }

    protected Integer getArchiveStorageId() {
		return archiveStorageId;
	}

	public void setArchiveStorageId(Integer archiveStorageId) {
		this.archiveStorageId = archiveStorageId;
	}

	protected Integer getIndexFileStorageId() {
		return indexFileStorageId;
	}

	public void setIndexFileStorageId(Integer indexFileStorageId) {
		this.indexFileStorageId = indexFileStorageId;
	}
    
	public static void saveAll(List<Metadata> updatedMetadata) {
		Ebean.save(updatedMetadata);
	}
	
	public static void updateAll(List<Metadata> data) throws Exception {
		Connection connectionObj = null;
		Statement stmt = null;
		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		
		try {
			Logger.info("Getting DB Connection");	
			connectionObj = DatabaseConnection.getUDASDBConnection() ;
			
			Logger.info("updating ILM_METADATA");
			connectionObj.setAutoCommit(false);

			String updateSQL = "update ILM_METADATA set STORAGE_URI = ?, EXTENDED_METADATA = ?, RETENTION_END = ?, EVENT_BASED = ?, MODIFIED_BY = ?, "
					+ "MODIFICATION_TIMESTAMP = ?, RECORD_CODE = ?, COUNTRY = ? where guid = ? and project_id = ?";
			preparedStatement = connectionObj.prepareStatement(updateSQL);

			for(Metadata a : data){
				preparedStatement.setString(1, a.getStorageUri());
				preparedStatement.setString(2, a.getExtendedMetadata());
				if(null == a.getRetentionEnd()){
					preparedStatement.setNull(3,java.sql.Types.INTEGER);
				}else{
					preparedStatement.setLong(3, a.getRetentionEnd());
				}
				preparedStatement.setString(4, a.getEventBased());
				preparedStatement.setString(5, a.getModifiedBy());
				preparedStatement.setLong(6, a.getModificationTimestamp());
				preparedStatement.setString(7, a.getRecordCode());
				preparedStatement.setString(8, a.getCountry());
				preparedStatement.setString(9, a.getGuid());
				preparedStatement.setString(10, a.getProjectId());
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			connectionObj.commit();
		} catch (SQLException e) {
			Logger.error("Error while updating ILM_METADATA in Db : "+e);
			e.printStackTrace();
			try {
				if(connectionObj != null)
					Logger.info("rolling back");
					connectionObj.rollback();
			} catch (SQLException e1) {
				Logger.error("Error while rollback updating ILM_METADATA in Db : "+e);
				e1.printStackTrace();
			}
			throw new Exception(e);
		} catch (Exception e) {
			Logger.error("Error while updating ILM_METADATA in Db : "+e);
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
	
	public static List<Metadata> getImpactedData(Integer cycleId) throws Exception{
		// select m.* from ILM_METADATA m, RETENTION_UPDATES r where m.record_code = r.record_code and m.country = r.country_code and r.cycle_Id = 301 and m.status != 'DELETED'
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RETENTION_UPDATES r "
				+ "where m.record_code = r.record_code "
				+ "and m.country = r.country_code "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> impactedData = executeQuery(query);
		return impactedData;
	}
	
	
	public static List<Metadata> getImpactedDataForRCA(Integer cycleId) throws Exception{
		// select m.* from ILM_METADATA m, RETENTION_UPDATES r where m.record_code = r.record_code and m.country = r.country_code and r.cycle_Id = 301 and m.status != 'DELETED'
		String query = "select m.ID, m.GUID, m.PROJECT_ID, m.AIT, " 
				+ "m.RECORD_CODE, m.COUNTRY, m.RETENTION_START, m.RETENTION_END, m.STATUS, m.MODIFICATION_TIMESTAMP, m.MODIFIED_BY, "
				+ "m.STORAGE_URI, m.EXTENDED_METADATA, m.METADATA_URL, "
				+ "m.EVENT_BASED, m.ARCHIVE_STORAGE_ID, m.INDEX_FILE_STORAGE_ID "
				+ "from ILM_METADATA m, RCA_UPDATES r "
				+ "where m.project_id = r.project_id "
				+ "and r.cycle_Id = "+cycleId+" "
				+ "and m.status != 'DELETED'";
		List<Metadata> impactedData = executeQuery(query);
		return impactedData;
	}
	
	private static List<Metadata> executeQuery(String query) throws Exception{
		List<Metadata> impactedData = new ArrayList<Metadata>();
		
/*		SqlQuery sqlQuery = Ebean.createSqlQuery(query);
		
		sqlQuery.setMaxRows(5000000);
	    List<SqlRow> sqlRows = sqlQuery.findList();

		Logger.info("No of impacted records "+sqlRows.size());
		
		for(SqlRow a : sqlRows){
			Metadata m = new Metadata();
			m.setId(a.getString("ID"));
			m.setGuid(a.getString("GUID"));
			m.setProjectId(a.getString("PROJECT_ID"));
			m.setAit(a.getString("AIT"));
			m.setRecordCode(a.getString("RECORD_CODE"));
			m.setCountry(a.getString("COUNTRY"));
			m.setRetentionStart(a.getLong("RETENTION_START"));
			m.setRetentionEnd(a.getLong("RETENTION_END"));
			m.setStatus(a.getString("STATUS"));
			m.setModificationTimestamp(a.getLong("MODIFICATION_TIMESTAMP"));
			m.setModifiedBy(a.getString("MODIFIED_BY"));
			m.setStorageUri(a.getString("STORAGE_URI"));
			m.setExtendedMetadata(a.getString("EXTENDED_METADATA"));
			m.setMetadataUrl(a.getString("METADATA_URL"));
			m.setEventBased(a.getString("EVENT_BASED"));
			impactedData.add(m);
		}*/
		
		Connection connectionObj = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
				Logger.info("Getting DB Connection");	
				connectionObj = DatabaseConnection.getUDASDBConnection() ;
				stmt = connectionObj.createStatement();
				rs = stmt.executeQuery(query);
				
				while(rs.next()){
					Metadata m = new Metadata();
					m.setId(rs.getString("ID"));
					m.setGuid(rs.getString("GUID"));
					m.setProjectId(rs.getString("PROJECT_ID"));
					m.setAit(rs.getString("AIT"));
					m.setRecordCode(rs.getString("RECORD_CODE"));
					m.setCountry(rs.getString("COUNTRY"));
					m.setRetentionStart(rs.getLong("RETENTION_START"));
					if(m.getRetentionStart() == 0){
						m.setRetentionStart(null);
					}
					m.setRetentionEnd(rs.getLong("RETENTION_END"));
					if(m.getRetentionEnd() == 0){
						m.setRetentionEnd(null);
					}
					m.setStatus(rs.getString("STATUS"));
					m.setModificationTimestamp(rs.getLong("MODIFICATION_TIMESTAMP"));
					m.setModifiedBy(rs.getString("MODIFIED_BY"));
					m.setStorageUri(rs.getString("STORAGE_URI"));
					m.setExtendedMetadata(rs.getString("EXTENDED_METADATA"));
					m.setMetadataUrl(rs.getString("METADATA_URL"));
					m.setEventBased(rs.getString("EVENT_BASED"));
					m.setArchiveStorageId(rs.getInt("ARCHIVE_STORAGE_ID"));
					m.setIndexFileStorageId(rs.getInt("INDEX_FILE_STORAGE_ID"));
					impactedData.add(m);
				}
			} catch (SQLException e) {
				Logger.error("Error while executing query on ILM_METADATA : "+e);
				e.printStackTrace();
				throw new Exception(e);
			} catch (Exception e) {
				Logger.error("Error while executing query on ILM_METADATA : "+e);
				e.printStackTrace();
				throw new Exception(e);
			}finally{
				DatabaseConnection.close(rs);
				DatabaseConnection.close(stmt);
				DatabaseConnection.close(connectionObj); 
				Logger.info("Connection closed");
			}
		return impactedData;
	}
}
