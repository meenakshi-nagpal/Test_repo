package models.storageapp;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import play.Logger;
import play.db.ebean.Model;
import utilities.Constant;
import utilities.Utility;
import adapters.aims.RecordCode.TimeUnit;
import adapters.cas.CenteraAdapter;

public class Helper {

	public static String getStorageUri(Metadata metadata) {
		if (metadata != null) return metadata.getStorageUri();
		else return "";
	}

	public static String getGuid(Metadata metadata) {
		if (metadata != null) return metadata.getGuid();
		else return "";
	}
	public static String getExtendedMetadata(Metadata metadata) {
		return metadata.getExtendedMetadata();
	}
	
	public static String getHistoryExtendedMetadata(MetadataHistory metadataHistory) {
		return metadataHistory.getIndexFileUriPrev();
	}
	
	public static String getExtendedMetadataContent(Metadata metadata, CenteraAdapter centeraAdapter) {
		if (metadata == null || metadata.getExtendedMetadata() == null || metadata.getExtendedMetadata().length() ==0) return "";

		File file = null;
        try {
			file = centeraAdapter.getAsFile(metadata.getExtendedMetadata(),metadata.getIsCompressed());
			if (file == null) return "";

			String content = new String(Files.readAllBytes(file.toPath()));
			return (content != null ? content.replaceAll("\n", "\t\t") : content);
		} catch (Exception e) {
			Logger.error("Helper - exception occurred while reading Centera file", e);
			e.printStackTrace();
			return "";
        } finally {
            if (file != null) {
                file.deleteOnExit();

                try {
                    Files.deleteIfExists(file.toPath());
                } catch (Exception e) {
        			Logger.error("Helper - exception occurred while deleting temporary file", e);
                    e.printStackTrace();
                }
            }
        }
	}
	
	public static String getMetadataId(Metadata metadata) {
		return metadata.getId();
	}
	
	public static Integer getArchiveStorageId(Metadata metadata) {
		return metadata.getArchiveStorageId();
	}
	
	public static Integer getIndexStorageId(Metadata metadata) {
		return metadata.getIndexFileStorageId();
	}
	
	public static String getCASStorageConnectionString(Integer storageID) {
		Storage storage = new Model.Finder<Integer, Storage>(
				Integer.class, Storage.class).byId(storageID);
		
		// Handle if there is failover to secondary storage
		if(storage != null) {
			if(storage.getStorageStatus() == Constant.STORAGE_FAILOVER_STATUS) {
				storage = new Model.Finder<Integer, Storage>(
						Integer.class, Storage.class).byId(
								storage.getBackupStorageId());
			}
		}
		
		String casNodeIPs = storage.getStorageUrl();
		String casPEAFileAbsolutePath = Utility.getAbsolutePEAFilePath(
				storage.getStorageAuth());
		
		String poolString = Utility.getCASPoolInfoString(casNodeIPs, 
				casPEAFileAbsolutePath);
		
		return poolString;
		
	}
	
	public static String getCASStorageConnectionStringbyStorage(Storage storage) {
		
		String casNodeIPs = storage.getStorageUrl();
		String casPEAFileAbsolutePath = Utility.getAbsolutePEAFilePath(
				storage.getStorageAuth());
		
		String poolString = Utility.getCASPoolInfoString(casNodeIPs, 
				casPEAFileAbsolutePath);
		
		return poolString;
		
	}
	
	public static String getArchiveStorageCASPoolString(Metadata metadata) {
		return getCASStorageConnectionString(metadata.getArchiveStorageId());
	}

	public static String getIndexFileStorageCASPoolString(Metadata metadata) {
		return getCASStorageConnectionString(metadata.getIndexFileStorageId());
	}
	
	public static String getHistoryIndexFileStorageCASPoolString(MetadataHistory metadatahistory) {
		return getCASStorageConnectionString(null);//metadatahistory.getIndexFileStorageId());
	}

	
	public static Storage getStorageConnectionDetails() {
	 	List<Storage> storageList=Storage.findCurrentWriteStorage();
	 	if(storageList==null) return null;
	 	
		Storage storage = storageList.get(0);
		
		return storage;
		
	}
	
	public static Storage getArchiveStorageDetails(Metadata data) {
		Integer archiveStorageId=getArchiveStorageId(data);
		Storage storage = new Model.Finder<Integer, Storage>(
				Integer.class, Storage.class).byId(archiveStorageId);
		
		// Handle if there is failover to secondary storage
		if(storage != null) {
			if(storage.getStorageStatus() == Constant.STORAGE_FAILOVER_STATUS) {
				storage = new Model.Finder<Integer, Storage>(
						Integer.class, Storage.class).byId(
								storage.getBackupStorageId());
			}
		}
		
		return storage;
	}
	
	public static Storage getIndexStorageDetails(Metadata data) {
		Integer archiveStorageId=getIndexStorageId(data);
		Storage storage = new Model.Finder<Integer, Storage>(
				Integer.class, Storage.class).byId(archiveStorageId);
		
		// Handle if there is failover to secondary storage
		if(storage != null) {
			if(storage.getStorageStatus() == Constant.STORAGE_FAILOVER_STATUS) {
				storage = new Model.Finder<Integer, Storage>(
						Integer.class, Storage.class).byId(
								storage.getBackupStorageId());
			}
		}
		
		return storage;
	}
	
	public static Integer getStorageType(Storage storage) {
		if(storage==null){return null;}
		
		return storage.getStorageType();
	}
	
	public static long getRecordCodeSeconds(Long retention, TimeUnit timeUnit) {
		if(retention==-1L){
			return -1;
		}else{
			return timeUnit.toSeconds(retention);
		}
	}
	
	public static TimeUnit getTimeUnit(String dispositionTimeUnit){
		TimeUnit timeUnit = TimeUnit.DAY; //Default
		if(dispositionTimeUnit.equalsIgnoreCase("Year(s)")){
			timeUnit=TimeUnit.YEAR;
		}
		else if(dispositionTimeUnit.equalsIgnoreCase("Month(s)")){
			timeUnit=TimeUnit.MONTH;
		}
		else if(dispositionTimeUnit.equalsIgnoreCase("Day(s)")){
			timeUnit=TimeUnit.DAY;
		}
		return timeUnit;
		
	}
	
	public static enum TimeUnit {
		YEAR(365L), 
		MONTH(365L / 12L),
		DAY(1L);

		
		private long days = 0L;
		private static long secondsPerDay = 60L * 60L * 24L;
		private static long millisecsPerDay = secondsPerDay * 1000L;

		TimeUnit(long days) {
			this.days = days;
		}

		public long retentionEnds(long maturityDate, long retention) {
			return maturityDate + toMillisecs(retention);
		}

		public long toSeconds(long retention) {
			return secondsPerDay * days * retention;
		}

		public long toMillisecs(long retention) {
			return millisecsPerDay * days * retention;
		}
	}
}