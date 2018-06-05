package jobs.aims;

import java.util.List;
import java.util.Map;

import adapters.AdapterException;
import adapters.aws.HCPAdapter;
import adapters.cas.CenteraAdapter;
import models.storageapp.Helper;
import models.storageapp.Metadata;
import models.storageapp.MetadataAnalysis;
import models.storageapp.MetadataHistory;
import models.storageapp.Storage;
import play.Logger;
import play.Play;
import utilities.Constant;
import utilities.Utility;

class ApplyUpdates implements Runnable {
	
	private static String CAS_DATE_FORMAT = Play.application().configuration().getString("cas.date.format");
	private static String appName = Play.application().configuration().getString("app.name");
	private static String appVersion = Play.application().configuration().getString("app.version");
	
	MetadataHistory history;
	List<Metadata> updatedMetadata;
	List<MetadataHistory> updatedHistoryData;
	List<MetadataAnalysis> updatedAnalysisData;
	Map<String, MetadataAnalysis> toBeUpdatedAnalysis;
	List<String> metadataIds;

	public ApplyUpdates(MetadataHistory history, List<Metadata> updatedMetadata, List<MetadataHistory> updatedHistoryData, 
			List<MetadataAnalysis> updatedAnalysisData, Map<String, MetadataAnalysis> toBeUpdatedAnalysis,List<String> metadataIds){
		this.history = history;
		this.updatedMetadata = updatedMetadata;
		this.updatedHistoryData = updatedHistoryData;
		this.updatedAnalysisData = updatedAnalysisData;
		this.toBeUpdatedAnalysis = toBeUpdatedAnalysis;
		this.metadataIds = metadataIds;
	}
	
	@Override
	public void run() {
		Metadata metadata = Metadata.findByGuid(history.getGuid(), history.getProjectId());
		if(metadata != null){
			if("no change".equalsIgnoreCase(history.getChangeType())){
				if(Constant.UNTRIGGERED.equalsIgnoreCase(history.getEbrComment()) || Constant.DISABLED.equalsIgnoreCase(history.getEbrComment())){
					metadata = updateEventBased(history.getUpdateType(),metadata, null);
					metadataIds.add(Helper.getMetadataId(metadata));
					
					if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
						metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
					}
					history.setStorageUriNew("");
					history.setIndexFileUriNew("");
					history.setDbUpdateFlag("Y");
					updatedHistoryData.add(history);
					updatedMetadata.add(metadata);
					
					MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
					a.setDbUpdateFlag("Y");
					updatedAnalysisData.add(a);
				}else if(!history.getRetentionTypePrev().equalsIgnoreCase(history.getRetentionTypeNew())){
					if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
						metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
						updatedMetadata.add(metadata);
					}
					history.setStorageUriNew("");
					history.setIndexFileUriNew("");
					history.setDbUpdateFlag("Y");
					updatedHistoryData.add(history);
					
					MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
					a.setDbUpdateFlag("Y");
					updatedAnalysisData.add(a);
				}else if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
					metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
					updatedMetadata.add(metadata);

					history.setStorageUriNew("");
					history.setIndexFileUriNew("");
					history.setDbUpdateFlag("Y");
					updatedHistoryData.add(history);
					
					MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
					a.setDbUpdateFlag("Y");
					updatedAnalysisData.add(a);
				}
				
				
				
			}else if("extend".equalsIgnoreCase(history.getChangeType()) && "R".equalsIgnoreCase(history.getStorageUriNew())){
				//create new centera clip, update ILM_METADATA and insert in ILM_METADATA_HISTORY
				Long recordCodePeriod = getRecordCodePeriod(history,metadata);
				Logger.info("getting new clip for guid "+metadata.getGuid());
				String clipId = getNewCenteraClip(metadata,recordCodePeriod,false);
				
				if(clipId != null){
					Logger.info("got new clip for guid "+metadata.getGuid());
					metadata.setStorageUri(clipId);
				} else {
					Logger.warn("could not get new clip for guid "+metadata.getGuid());
					clipId = "NA";
					history.setExtensionStatus("FAILED");
				}
				if("R".equalsIgnoreCase(history.getIndexFileUriNew())){
					//Index file is in centera
					String indexClipId = getNewCenteraClip(metadata,recordCodePeriod,true);
					if(indexClipId != null){
						Logger.info("got new clip for index file for guid "+metadata.getGuid());
						metadata.setExtendedMetadata(indexClipId);
					}else{
						Logger.warn("could not get new clip for index file for guid "+metadata.getGuid());
						indexClipId = "NA";
					}
					history.setIndexFileUriNew(indexClipId);
				} else if("ER".equalsIgnoreCase(history.getIndexFileUriNew())){
					//Index file is in HCP, apply the extended retention end date in HCP
					applyRetentionEndDateInHCP(history, metadata, false);	
					history.setIndexFileUriNew("");
				} else{
					history.setIndexFileUriNew("");
				}
				
				metadata = updateRetentionEnd(history.getUpdateType(), metadata, history.getRetentionEndNew());
				if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
					metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
				}
				history.setStorageUriNew(clipId);
				history.setDbUpdateFlag("Y");
				updatedHistoryData.add(history);
				updatedMetadata.add(metadata);
				
				MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
				a.setDbUpdateFlag("Y");
				updatedAnalysisData.add(a);
				
				
			}else if("shorten".equalsIgnoreCase(history.getChangeType()) || ("extend".equalsIgnoreCase(history.getChangeType()) && "NR".equalsIgnoreCase(history.getStorageUriNew())) ||
					("extend".equalsIgnoreCase(history.getChangeType()) && "ENR".equalsIgnoreCase(history.getStorageUriNew()))){
				metadata = updateRetentionEnd(history.getUpdateType(),metadata, history.getRetentionEndNew());
				//if(metadata != null){
					if(Constant.UNTRIGGERED.equalsIgnoreCase(history.getEbrComment()) || Constant.DISABLED.equalsIgnoreCase(history.getEbrComment())){
						metadata.setEventBased(null);
						metadataIds.add(Helper.getMetadataId(metadata));
					}
					if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
						metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
					}
					history.setStorageUriNew("");
					history.setIndexFileUriNew("");
					history.setDbUpdateFlag("Y");
					updatedHistoryData.add(history);
					updatedMetadata.add(metadata);
					
					MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
					a.setDbUpdateFlag("Y");
					updatedAnalysisData.add(a);
				//}
			}else if("extend".equalsIgnoreCase(history.getChangeType()) && "ER".equalsIgnoreCase(history.getStorageUriNew())){
				Storage archiveStorage = Storage.findById(Helper.getArchiveStorageId(metadata));
				if(archiveStorage == null){
					Logger.warn("No storage layer associated with this archive storage id.");
				}else if(archiveStorage.getStorageType() == Constant.STORAGE_TYPE_HCP_ID){
					//update retention end date in HCP for archive files
					applyRetentionEndDateInHCP(history, metadata, true);
				}
				if("ER".equalsIgnoreCase(history.getIndexFileUriNew())){
					//Index file is in HCP, apply the extended retention end date in HCP
					applyRetentionEndDateInHCP(history, metadata, false);	
					history.setIndexFileUriNew("");
				}else if("R".equalsIgnoreCase(history.getIndexFileUriNew())){
					//Index is in Centera
					Long recordCodePeriod = getRecordCodePeriod(history,metadata);
					String indexClipId = getNewCenteraClip(metadata,recordCodePeriod,true);
					if(indexClipId != null){
						Logger.info("got new clip for index file for guid "+metadata.getGuid());
						metadata.setExtendedMetadata(indexClipId);
					}else{
						Logger.warn("could not get new clip for index file for guid "+metadata.getGuid());
						indexClipId = "NA";
					}
					history.setIndexFileUriNew(indexClipId);
				}else{
					history.setIndexFileUriNew("");
				} 
					
				history.setStorageUriNew("");					
				metadata = updateRetentionEnd(history.getUpdateType(),metadata, history.getRetentionEndNew());
				if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(history.getUpdateType())){
					metadata = updateRecordCode(metadata, history.getRecordCodeNew(),history.getCountryNew());
				}
				history.setDbUpdateFlag("Y");
				updatedHistoryData.add(history);
				updatedMetadata.add(metadata);
				
				MetadataAnalysis a = toBeUpdatedAnalysis.get(history.getGuid()+","+history.getCycleId());
				a.setDbUpdateFlag("Y");
				updatedAnalysisData.add(a);
			}
		}else{
			Logger.info("No ILM_METADATA found for this ILM_METADATA_ANALYSIS guid "+history.getGuid());
		}
		
	}
	
	private static Metadata updateRetentionEnd(String updateType,Metadata metadata,Long retentionEndNew) {
		metadata.setRetentionEnd(retentionEndNew);
		if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(updateType)){
			metadata.setModifiedBy("ApplyRecordCodeAlignmentDetailsJob");
		}else if(Constant.RETENTION_SCHEDULE_AUTOMATION.equalsIgnoreCase(updateType)){
			metadata.setModifiedBy("ApplyRetentionUpdatesJob");
		}
    	metadata.setModificationTimestamp(Utility.getCurrentTime());
		return metadata;
	}

	private static Metadata updateEventBased(String updateType, Metadata metadata,String eventBased) {
		metadata.setEventBased(eventBased);
		if(Constant.RECORD_CODE_ALIGNMENT.equalsIgnoreCase(updateType)){
			metadata.setModifiedBy("ApplyRecordCodeAlignmentDetailsJob");
		}else if(Constant.RETENTION_SCHEDULE_AUTOMATION.equalsIgnoreCase(updateType)){
			metadata.setModifiedBy("ApplyRetentionUpdatesJob");
		}
    	
    	metadata.setModificationTimestamp(Utility.getCurrentTime());
		return metadata;
	}
	
	private static Metadata updateRecordCode(Metadata metadata,String recordCodeNew, String countryCodeNew) {
		metadata.setRecordCode(recordCodeNew);
		metadata.setCountry(countryCodeNew);
		metadata.setModifiedBy("ApplyRecordCodeAlignmentDetailsJob");
    	metadata.setModificationTimestamp(Utility.getCurrentTime());
		return metadata;
	}
	
	private static void applyRetentionEndDateInHCP(MetadataHistory history, Metadata metadata,boolean forArchiveFile){
		
		Storage storage = null;
		if(forArchiveFile){
			storage = Storage.findById(Helper.getArchiveStorageId(metadata));
		}else{
			storage = Storage.findById(Helper.getIndexStorageId(metadata));
		}
		if(storage == null){
			Logger.warn("No storage layer associated with this guid"+metadata.getGuid());
			return;
			
		}else if(storage.getStorageType() == Constant.STORAGE_TYPE_HCP_ID){
			String hcpEndUrl=storage.getHcpRestUrl();
			String accessKey=storage.getAccessKey();
			String secretKey=storage.getSecretKey();
									
			try {
				Long newRetentionEnd = history.getRetentionEndNew();
				if(newRetentionEnd == null){
					newRetentionEnd = -1L;
				}else{
					newRetentionEnd = newRetentionEnd / 1000L;
				}
				HCPAdapter hcpAdapter=new HCPAdapter(hcpEndUrl,accessKey,secretKey);
				if(forArchiveFile){
					hcpAdapter.applyRetentionEndDate(metadata.getGuid(),newRetentionEnd);
				}else {
					String indexFileName=metadata.getGuid()+Constant.HCP_INDEX_FILE_NAME_PREFIX; 
					hcpAdapter.applyRetentionEndDate(indexFileName,newRetentionEnd);
				}
				
			} catch (AdapterException e) {
				Logger.warn("Error occurred while applying retention end date in HCP");
				if(forArchiveFile){
					history.setExtensionStatus("FAILED");
				}
				
			}
		}
	}
	
	private static Long getRecordCodePeriod(MetadataHistory history, Metadata metadata) {
		//get recordcode period by substracting start date from end date
		if("Permanent".equalsIgnoreCase(history.getRetentionTypeNew())){
			return -1L;
		}else{
			return (history.getRetentionEndNew() - metadata.getRetentionStart()) / 1000L;
		}
	}
	
	private static String getNewCenteraClip(Metadata metadata, Long recordCodePeriod, boolean forIndex) {
		String clipId = null;
		CenteraAdapter archiveAdapter = null;
		String uri = null;
		try{
			if(forIndex){
				uri = Helper.getExtendedMetadata(metadata);
				if(uri != null && uri.length() > 0) {
					String archivePoolString = Helper.getIndexFileStorageCASPoolString(metadata);
					archiveAdapter = new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT, appName, appVersion);
				}	
			}else {
				uri = Helper.getStorageUri(metadata);	
				if(uri != null && uri.length() > 0) {
					String archivePoolString = Helper.getArchiveStorageCASPoolString(metadata);
					archiveAdapter = new CenteraAdapter(archivePoolString, CAS_DATE_FORMAT,	appName, appVersion);
				}	
			}
		}catch (Exception e) {
			Logger.error("Error occurred while getting centera pool info "+e);
		}
		//CenteraClip clip;
		if(archiveAdapter != null){
			try {
				//Logger.info("************"+Thread.currentThread().toString() + uri);
				//clip = archiveAdapter.get(uri);
				//Logger.info(">>>>>>>>>>>> returned from get(storageuri) "+Thread.currentThread().toString() + clip.clipid);
				//clip.modificationDate = System.currentTimeMillis();
				
				//clip.setRetentionPeriod(metadata.getRetentionStart(), recordCodePeriod);
				
				//clipId =archiveAdapter.update(clip);
				clipId = archiveAdapter.updateRetention(uri, metadata.getRetentionStart(), recordCodePeriod);
				//Logger.info(">>>>>>>>>>>> returned from updateRetention "+Thread.currentThread().toString() + clipId);

			} catch (Exception e) {
				Logger.error("Error occurred while updating centera clip "+e);
			}
		}
		return clipId;
	}
}
