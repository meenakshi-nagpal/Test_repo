package models.storageapp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import java.util.List;

import utilities.Constant;
import play.db.ebean.Model;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Expr;
import com.avaje.ebean.Query;
@Entity
@Table(name="STORAGE_MASTER")
public class Storage extends Model {

	private static final long serialVersionUID = 403981067996357986L;

	@Id
	@Column(name = "ID")
	private Integer id;
	
	@Column(name = "STORAGE_TYPE")
	private Integer storageType;
	
	@Column(name = "STORAGE_URL")
	private String storageUrl;
	
	@Column(name = "STORAGE_AUTH")
	private String storageAuth;
	
	@Column(name = "STORAGE_STATUS")
	private Integer storageStatus;

	@Column(name = "BACKUP_STORAGE_ID")
	private Integer backupStorageId;

	@Column(name = "BUCKET_NAME")
	private String bucketName;
	
	@Column(name = "HCP_S3_URL")
	private String hcpS3Url;
	
	@Column(name = "HCP_REST_URL")
	private String hcpRestUrl;
	
	@Column(name = "ACCESS_KEY")
	private String accessKey;
	
	@Column(name = "SECRET_KEY")
	private String secretKey;
	
	@Column(name = "STORAGE_CATEGORY_ID")
	private Integer storageCategoryId;
	
	
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getStorageType() {
		return storageType;
	}

	public void setStorageType(Integer storageType) {
		this.storageType = storageType;
	}

	public String getStorageUrl() {
		return storageUrl;
	}

	public void setStorageUrl(String storageUrl) {
		this.storageUrl = storageUrl;
	}

	public String getStorageAuth() {
		return storageAuth;
	}

	public void setStorageAuth(String storageAuth) {
		this.storageAuth = storageAuth;
	}

	public Integer getStorageStatus() {
		return storageStatus;
	}

	public void setStorageStatus(Integer storageStatus) {
		this.storageStatus = storageStatus;
	}

	public Integer getBackupStorageId() {
		return backupStorageId;
	}

	public void setBackupStorageId(Integer backupStorageId) {
		this.backupStorageId = backupStorageId;
	}
	
	
	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public String getHcpS3Url() {
		return hcpS3Url;
	}

	public void setHcpS3Url(String hcpS3Url) {
		this.hcpS3Url = hcpS3Url;
	}
	
	public String getHcpRestUrl() {
		return hcpRestUrl;
	}

	public void setHcpRestUrl(String hcpRestUrl) {
		this.hcpRestUrl = hcpRestUrl;
	}
	
	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}
	
	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public Integer getStorageCategoryId() {
		return storageCategoryId;
	}

	public void setStorageCategoryId(Integer storageCategoryId) {
		this.storageCategoryId = storageCategoryId;
	}
	
	//DB Operations beyond this
	public static Finder<String, Storage> find = new Finder(String.class,Storage.class);
	
	public static List<Storage> findCurrentWriteStorage()
	{
		Query<Storage> query = Ebean.createQuery(Storage.class);
		query.where(Expr.and(Expr.eq("storageCategoryId",Constant.STORAGE_CATEGORY_REGULATORY_R_W_ID),
		    		 Expr.isNotNull("backupStorageId")));
		List<Storage> storageList = query.findList();//Storage.find.where().eq("storageCategoryId", Constant.STORAGE_CATEGORY_REGULATORY_R_W_ID).orderBy("id").findList();
		if(storageList.size()==1){
			return storageList;
		}
		return null;
	} 
	
	public static Storage findById(Integer id){
		Storage storage = (Storage)find.where().eq("id", id).findUnique();
		return storage;
	}
	
	public static List<Storage> findAll(){
		List<Storage> storage = (List<Storage>)find.all();
		return storage;
	}
	
	@Override
	public String toString() {
		return "Storage [id=" + id
				+ ", storageType=" + storageType + ", storageUrl="
				+ storageUrl + ", storageAuth="
				+ storageAuth + ", storageStatus=" + storageStatus
				+ ", backupStorageId=" + backupStorageId
				+ ", bucketName=" + bucketName + ", hcpS3Url="
				+ hcpS3Url+", hcpS3Url="+hcpS3Url+" ]";
	}
}
