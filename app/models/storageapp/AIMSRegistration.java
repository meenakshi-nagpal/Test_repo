package models.storageapp;


import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;

import play.Logger;
import play.db.ebean.Model;
import play.db.ebean.Model.Finder;
import utilities.Constant;
import valueobjects.ArchiveFileRegistrationVO;

@Entity
@Table(name="AIMS_REGISTRATION")
public class AIMSRegistration extends Model {

	//private static final Logger.ALogger LOGGER = Logger.of(AIMSRegistration.class);

	private static final long serialVersionUID = 9200957015466336481L;

	/*	@SequenceGenerator(name="UDASAIMSMapSEQ",sequenceName="UDAS_TO_AIMS_MAPPING_SEQ", 
			allocationSize=1) 
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator="UDASAIMSMapSEQ")
	@Id
	@Column(name = "ID")
	private Long id;*/

	@Id
	@Column(name = "METADATA_ID")
	private String id;

	/*	@OneToOne(optional = false)
	@JoinColumn(name = "UDAS_METADATA_ID")
	private Metadata udasMetadata;*/

	@Column(name = "AIMS_GUID")
	private String aimsGuid;

	@Column(name = "STATUS")
	private Integer status;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	/*	public Metadata getUdasMetadata() {
		return udasMetadata;
	}

	public void setUdasMetadata(Metadata udasMetadata) {
		this.udasMetadata = udasMetadata;
	}*/

	public String getAimsGuid() {
		return aimsGuid;
	}

	public void setAimsGuid(String aimsGuid) {
		this.aimsGuid = aimsGuid;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	public static AIMSRegistration newInstance(
			ArchiveFileRegistrationVO archiveFileRegistrationVO, Metadata metadata) {

		Logger.debug("Creating new object instance, ID: " + metadata.getId());
		AIMSRegistration aimsRegistration = new AIMSRegistration();

		aimsRegistration.setId(metadata.getId());
		//aimsRegistration.setUdasMetadata(metadata);

		if(archiveFileRegistrationVO != null) {
			aimsRegistration.setAimsGuid(archiveFileRegistrationVO.getAimsGuid());
			aimsRegistration.setStatus(Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_SUCCESS);
		} else {
			aimsRegistration.setStatus(Constant.AIMS_REGISTRATION_STATUS_ARCHIVE_FILE_REGISTRATION_FAILED);
		}

		return aimsRegistration;
	}

	public static AIMSRegistration findById(String id) {
		return Ebean.find(AIMSRegistration.class, id);
	}

	//DB operations
	public static Finder<Long, AIMSRegistration> find = new Finder(Long.class,
			AIMSRegistration.class);

	public static AIMSRegistration getUdasIdbyAimsGuid(String aimsGuid)	{
		return find.where().eq("AIMS_GUID", aimsGuid).findUnique();
	}

    public static List<AIMSRegistration> getAIMSRegsWithIndexUpdatesFailed(){
		
		List<AIMSRegistration> aimsRegList = find
				.where().eq(Constant.AIMS_REGISTRATION_STATUS_KEY, Constant.AIMS_REGISTRATION_STATUS_INDEX_REGISTRATION_FAILED).findList();
		return 	aimsRegList;
	}

    
    public static List<AIMSRegistration> getAIMSRegsWithEventBasedUpdatePending(){
		
		/*List<AIMSRegistration> aimsRegList = find
				.where().eq(Constant.AIMS_REGISTRATION_STATUS_KEY, Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_PENDING).findList();*/
    	List<AIMSRegistration> aimsRegList = find
				.where().or(Expr.eq(Constant.AIMS_REGISTRATION_STATUS_KEY, Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_PENDING),
							Expr.eq(Constant.AIMS_REGISTRATION_STATUS_KEY, Constant.AIMS_REGISTRATION_STATUS_EVENT_BASED_UPDATE_FAILED)).findList();
		return 	aimsRegList;
	}
   	@Override
	public String toString() {
		return "AIMSRegistration [id=" + id + ", aimsGuid=" + aimsGuid
				+ ", status=" + status + "]";
	}
	
	

}
