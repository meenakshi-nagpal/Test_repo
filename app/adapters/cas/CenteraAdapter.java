package adapters.cas;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.Deflater;


import java.util.zip.ZipException;

import org.apache.commons.compress.compressors.CompressorException;

import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPPoolQuery;
import com.filepool.fplibrary.FPQueryExpression;
import com.filepool.fplibrary.FPQueryResult;
import com.filepool.fplibrary.FPLibraryConstants;
import com.filepool.fplibrary.FPTag;

import adapters.AdapterException;
import utilities.DateUtil;
import compression.CompressInputStream;
import compression.CompressionUtility;

public class CenteraAdapter {

	private static final Logger log = Logger.getLogger(CenteraAdapter.class.getName());

	private String casPoolInfo;
	private String dateFormat = "yyyy.MM.dd HH:mm:ss z";
	public String appName;
	public String appVersion;

	public CenteraAdapter(String casPoolInfo, String dateFormat,
			String appName, String appVersion) {

		if(casPoolInfo == null || appName == null || appVersion == null ||
				casPoolInfo.trim().isEmpty() || appName.trim().isEmpty() ||
				appVersion.trim().isEmpty()) {
			throw new IllegalArgumentException("poolAddress or appName or appVersion" +
					" cannot be null or empty");
		}
		
		this.casPoolInfo = casPoolInfo;
		if(dateFormat != null) {
			this.dateFormat = dateFormat;
		}
		this.appName = appName;
		this.appVersion = appVersion;
	}

	public CenteraClip put(CenteraClip centeraClip) throws AdapterException {
		FPPool fpPool = null;
		FPClip fpClip = null;
		FPTag fpTopTag = null;
		FPTag fpNewTag = null;
		InputStream inputStream = null;

		try {
			fpPool = openPool();
			log.info("Thread:" + Thread.currentThread().getName()
					+ "; Writing To Pool: " + fpPool);
			fpClip = new FPClip(fpPool, centeraClip.filename);
			fpClip.setDescriptionAttribute("x-appname", appName);
			fpClip.setDescriptionAttribute("x-recordcode", centeraClip.recordCode);
			fpClip.setDescriptionAttribute("x-country", centeraClip.country);
			fpClip.setDescriptionAttribute("x-maturitydate", String.valueOf(centeraClip.maturityDate));
			fpClip.setDescriptionAttribute("x-recordcodeperiod", String.valueOf(centeraClip.recordCodePeriod));
			fpClip.setDescriptionAttribute("x-archiveserver", casPoolInfo);
			if (centeraClip.projectid != null && centeraClip.projectid.length() != 0) fpClip.setDescriptionAttribute("x-projectid", centeraClip.projectid);
			if (centeraClip.guid != null && centeraClip.guid.length() != 0) fpClip.setDescriptionAttribute("x-guid", centeraClip.guid);
			if (centeraClip.creationUser != null && centeraClip.creationUser.length() != 0) fpClip.setDescriptionAttribute("x-username", centeraClip.creationUser);
			if (centeraClip.ait != null && centeraClip.ait.length() != 0) fpClip.setDescriptionAttribute("x-ait", centeraClip.ait);
			fpClip.setRetentionPeriod(centeraClip.getRetentionPeriod());
			

			fpTopTag = fpClip.getTopTag();
			fpNewTag = new FPTag(fpTopTag, centeraClip.getTagname());

			inputStream = centeraClip.getInputStream();
			if(centeraClip.getCompressionEnabled().equalsIgnoreCase("true")){
				InputStream compressedInput = new CompressInputStream(inputStream); 

				fpNewTag.BlobWrite(compressedInput);
			}else{
				fpNewTag.BlobWrite(inputStream);
			}


			centeraClip = get(fpClip.Write());
			
			log.info("Thread:" + Thread.currentThread().getName()
					+ "; Wrote To Pool: " + fpPool +
					"; C-Clip ID: " + centeraClip);

			return centeraClip;

		} catch (FPLibraryException e) {
			log.severe("Centera SDK Put Error: " + e.getMessage());
			throw new AdapterException(e);
		} catch (FileNotFoundException e) {
			log.severe("File Not Found Error: " + e.getMessage());
			throw new AdapterException(e);
		} catch (IOException e) {
			log.severe("IO Put Error: " + e.getMessage());
			throw new AdapterException(e);
		} catch (NumberFormatException e) {
			log.severe("Error to parse retention period value: " + e.getMessage());
			throw new AdapterException(e);
		} finally {
			closeInputStream(inputStream);
			closeTag(fpNewTag);
			closeTag(fpTopTag);
			closeClip(fpClip);
			closePool(fpPool);
		}
	}
	
	public String updateRetention(final String clipIDToUpdate, Long startDate, Long recordCodePeriod) throws FPLibraryException {
		FPPool fpPool = null;
		fpPool = openPool();
		final FPClip updatedClip = new FPClip(fpPool, clipIDToUpdate, FPLibraryConstants.FP_OPEN_ASTREE);
	
		long retentionPeriod;
		long currentTime = System.currentTimeMillis();
		if (startDate <= 0) {
			startDate = currentTime;
		} 		
		if(recordCodePeriod<0L){
			retentionPeriod = -1L;
		}
		else{
			long end = startDate + recordCodePeriod * 1000L;	
			long remain = end - currentTime;
			if (remain <= 0L) {
				retentionPeriod = (0L);
			} else {
				retentionPeriod = (remain / 1000L);
			}
		}
				
        updatedClip.setRetentionPeriod(retentionPeriod);
        updatedClip.setDescriptionAttribute("x-recordcodeperiod", String.valueOf(recordCodePeriod));
        updatedClip.setDescriptionAttribute("x-modificationdate", String.valueOf(System.currentTimeMillis()));
		
        final String newClipId = updatedClip.Write();

        updatedClip.Close();

        return newClipId;
 }


	/*public String update(CenteraClip centeraClip) throws AdapterException {
		FPPool fpPool = null;
		FPClip fpClip = null;
		//FPTag fpTag = null;
		
		FPTag fpTopTag = null;
		FPTag fpNewTag = null;
		InputStream inputStream = null;
		
		try {
			fpPool = openPool();

			//if (!FPClip.Exists(fpPool, centeraClip.clipid)) return null;

			fpClip = new FPClip(fpPool, centeraClip.filename);	
			//fpTag = fpClip.getTopTag();
			
			fpClip.setDescriptionAttribute("x-appname", appName);
			fpClip.setDescriptionAttribute("x-recordcode", centeraClip.recordCode);
			fpClip.setDescriptionAttribute("x-country", centeraClip.country);
			fpClip.setDescriptionAttribute("x-maturitydate", String.valueOf(centeraClip.maturityDate));
			fpClip.setDescriptionAttribute("x-modificationdate", String.valueOf(centeraClip.modificationDate));
			fpClip.setDescriptionAttribute("x-recordcodeperiod", String.valueOf(centeraClip.recordCodePeriod));
			fpClip.setDescriptionAttribute("x-archiveserver", casPoolInfo);
			
			if (centeraClip.projectid != null && centeraClip.projectid.length() != 0) fpClip.setDescriptionAttribute("x-projectid", centeraClip.projectid);
			if (centeraClip.guid != null && centeraClip.guid.length() != 0) fpClip.setDescriptionAttribute("x-guid", centeraClip.guid);
			if (centeraClip.creationUser != null && centeraClip.creationUser.length() != 0) fpClip.setDescriptionAttribute("x-username", centeraClip.creationUser);
			if (centeraClip.ait != null && centeraClip.ait.length() != 0) fpClip.setDescriptionAttribute("x-ait", centeraClip.ait);
			fpClip.setRetentionPeriod(centeraClip.getRetentionPeriod());
			
			char isCompressed = centeraClip.getCompressionEnabled().equalsIgnoreCase("true")?'Y':'N';
			centeraClip.setInputPath(centeraClip.getOutputPath(centeraClip.clipid, isCompressed, this));
			
			fpTopTag = fpClip.getTopTag();
			fpNewTag = new FPTag(fpTopTag, centeraClip.getTagname());

			inputStream = centeraClip.getInputStream();
			if(centeraClip.getCompressionEnabled().equalsIgnoreCase("true")){
				InputStream compressedInput = new CompressInputStream(inputStream); 

				fpNewTag.BlobWrite(compressedInput);
			}else{
				fpNewTag.BlobWrite(inputStream);
			}
			
			String updatedClipId = fpClip.Write();
			log.info("Thread:" + Thread.currentThread().getName()
					+ "; Wrote To Pool: " + fpPool +
					"; NEW-Clip ID: " + updatedClipId);
			/*if (FPClip.Exists(fpPool, centeraClip.clipid))
				System.out.println("***************old clip still exists : "+ centeraClip.clipid);
			
			if (FPClip.Exists(fpPool, updatedClipId))
				System.out.println("********************new clip also exists : "+ updatedClipId);
			*/
			/*return updatedClipId;
		} catch (FPLibraryException e) {
			log.severe("Centera SDK UPDATE API Error: " + e.getMessage());
			throw new AdapterException(e);
		} catch (IOException e) {
			log.severe("IO Put Error: " + e.getMessage());
			throw new AdapterException(e);
		} finally {
			closeInputStream(inputStream);
			closeTag(fpNewTag);
			closeTag(fpTopTag);
			closeClip(fpClip);
			closePool(fpPool);
		}		
	}*/
	
	public List<CenteraClip> list(final long offset, final long length) throws AdapterException {
		List<CenteraClip> list = Collections.emptyList();

		FPPool fpPool = null;
		FPQueryExpression fpQueryExp = null;
		FPPoolQuery fpPoolQuery = null;
		FPQueryResult fpQueryResult = null;
		int fpQueryStatus = 0;

		try {
			fpPool = openPool(true);

			fpQueryExp = new FPQueryExpression();

			fpQueryExp.setType(FPLibraryConstants.FP_QUERY_TYPE_EXISTING);

			fpPoolQuery = new FPPoolQuery(fpPool, fpQueryExp);

			long count = 1;
			final long max = offset + length;
			while (count < max) {
				if (list.isEmpty()) list = new ArrayList<CenteraClip>();;

				fpQueryResult = fpPoolQuery.FetchResult();

				fpQueryStatus = fpQueryResult.getResultCode();

				if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_OK) {
					String clipid = fpQueryResult.getClipID();
					if (count >= offset) {
						CenteraClip clip = new CenteraClip(clipid);
						list.add(clip);						
					}
					count++;
				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_INCOMPLETE) {
					// Error occured one or more nodes on centera could not be queried.
					log.severe("Received FP_QUERY_RESULT_CODE_INCOMPLETE error, invalid C-Clip, trying again.");

				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_COMPLETE) {
					// Indicate error should have been received after incomplete error
					log.severe("Received FP_QUERY_RESULT_CODE_COMPLETE, there should have been a previous FP_QUERY_RESULT_CODE_INCOMPLETE error reported.");

				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_END) {
					// all results have been received finish query.
					//System.out.println("End of query reached, exiting.");
					break;

				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_ABORT) {
					// query aborted due to server side issue or start time
					// is later than server time.
					log.severe("received FP_QUERY_RESULT_CODE_ABORT error, exiting.");
					break;

				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_ERROR) {
					//Server error
					log.severe("received FP_QUERY_RESULT_CODE_ERROR error, retrying again");

				} else if (fpQueryStatus == FPLibraryConstants.FP_QUERY_RESULT_CODE_PROGRESS) {
					//System.out.println("received FP_QUERY_RESULT_CODE_PROGRESS, continuing.");

				} else {
					// Unknown error, stop running query
					log.severe("received error: " + fpQueryStatus);
					break;
				}

				fpQueryResult.Close();

			} //while

		} catch (Exception e) {
			log.severe("Centera SDK Error: " + e.getMessage());	
			throw new AdapterException(e);
		} finally {
			closeQueryResult(fpQueryResult);
			closePoolQuery(fpPoolQuery);
			closeQueryExpression(fpQueryExp);
			closePool(fpPool);
		}

		return list;
	}

	public void read(String clipId,char isCompressed, OutputStream outputStream) throws AdapterException {
		FPPool fpPool = null;
		FPClip fpClip = null;
		FPTag fpTag = null;
		try {
			fpPool = openPool();

			log.info("Thread:" + Thread.currentThread().getName()
					+ "; Reading From Pool: " + fpPool +
					"; C-Clip ID: " + clipId);
			if (!FPClip.Exists(fpPool, clipId)) return;


			fpClip = new FPClip(fpPool, clipId, FPLibraryConstants.FP_OPEN_FLAT);	
			fpTag = fpClip.getTopTag();
			
			if(isCompressed=='Y'){
//				String temploc=play.Play.application().configuration().getString("temp.location.dir");
				String temploc=System.getenv("TEMP_LOC_DIR");
				String fileName= temploc + "/" + clipId + ".zip";
				log.info("templocation:"+fileName);
				OutputStream fos = new FileOutputStream(fileName);
				fpTag.BlobRead(fos);
				File file = new File(fileName);
				FileInputStream fin = new FileInputStream(file);
				CompressionUtility.decompress(outputStream,fin);
				try{
					fin.close();
					fos.close();
				}catch(IOException ie){
					
				}
				file.delete();
			}else{
				fpTag.BlobRead(outputStream);
			}		

			return;
		} catch (FPLibraryException e) {
			log.severe("Centera SDK GET API Error: " + e.getMessage());
			throw new AdapterException(e);
		} catch (IOException e) {
			log.severe("IO Error: " + e.getMessage());
			throw new AdapterException(e);
		} finally {
			closeTag(fpTag);
			closeClip(fpClip);
			closePool(fpPool);
		}		
	}

	public CenteraClip get(String clipid) throws AdapterException {
		FPPool fpPool = null;
		FPClip fpClip = null;
		FPTag fpTag = null;
		java.io.ByteArrayOutputStream os = null;
		try {
			fpPool = openPool();

			if (!FPClip.Exists(fpPool, clipid)) return null;

			fpClip = new FPClip(fpPool, clipid, FPLibraryConstants.FP_OPEN_FLAT);

			CenteraClip clip = new CenteraClip(fpClip.getClipID());
			clip.setRetentionPeriod(fpClip.getRetentionPeriod());
			clip.modified = fpClip.IsModified();
			clip.eventBasedRetention = fpClip.isEBREnabled();
			clip.retentionHold = fpClip.getRetentionHold();
			clip.retentionClassName = fpClip.getRetentionClassName();

			String[] attrs = fpClip.getDescriptionAttributes();
			clip.attributes = new HashMap<String, String>();
			for(int i=0; i<attrs.length;){
				String key = attrs[i++];
				String val = attrs[i++];
				if ("name".equals(key)) {
					clip.filename = val;
				} else if ("totalsize".equals(key)) {
					clip.size = Long.parseLong(val);
				} else if ("creation.profile".equals(key)) {
					clip.profile = val;					
				} else {
					clip.attributes.put(key, val);
				}

				if ("creation.date".equals(key)) {
					try {
						clip.creationDate = DateUtil.parseStringToTime(dateFormat, val);
					} catch (Exception e) {
						clip.creationDate = 0L;
					}
				} else if ("x-guid".equals(key)) {
					clip.guid = val;
				} else if ("modification.date".equals(key)) {
					try {
						clip.modificationDate = DateUtil.parseStringToTime(dateFormat, val);
					} catch (Exception e) {
						clip.modificationDate = clip.creationDate;
					}
				} else if ("x-projectid".equals(key)) {
					clip.projectid = val;
				} else if ("x-username".equals(key)) {
					clip.creationUser = val;
				} else if ("x-recordcode".equals(key)) {
					clip.recordCode = val;
				} else if ("x-country".equals(key)) {
					clip.country = val;
				} else if ("x-recordcodeperiod".equals(key)) {
					clip.recordCodePeriod = Long.parseLong(val);
				} else if ("x-maturitydate".equals(key)) {
					clip.maturityDate = Long.parseLong(val);
				} else if ("x-archiveserver".equals(key)) {
					clip.server = val;
				}
			}

			return clip;
		} catch (FPLibraryException e) {
			log.severe("Centera SDK VIEW API Error: " + e.getMessage());
			throw new AdapterException(e);
		} 
		finally {
			closeOutputStream(os);
			closeTag(fpTag);
			closeClip(fpClip);
			closePool(fpPool);
		}		
	}

	public File getAsFile(String clipid,char isCompressed) throws AdapterException {
		OutputStream  out = null;
		Path path = null;
		try {
			if (exists(clipid)) {
				path = Files.createTempFile("cas", null);
				out = Files.newOutputStream(path);                
				read(clipid, isCompressed, out);

				return path.toFile();               
			} else {
				return null;
			}
		} catch (Exception e) {
			log.severe("Centera SDK GET AS FILE API Error: " + e.getMessage());
			throw new AdapterException(e);
		} finally {
			try {
				if (out != null) out.close();
			} catch (IOException e) {}
		}
	} 

	public int delete(String clipid){
		try {
			doDelete(clipid, null);
		} catch (AdapterException e) {
			log.severe("Could not delete archive. " + e.getMessage());
			return -1;
		}
		catch(Exception e)
		{
			log.severe("Could not delete archive. " + e.getMessage());
			return -1;
		}
		return 1; 
	}

	public int auditedDelete(String clipid, String reason) throws AdapterException {
		try {
			doDelete(clipid, reason);
		} catch (AdapterException e) {
			log.severe("Could not delete archive. " + e.getMessage());
			return -1;
		}
		catch(Exception e)
		{
			log.severe("Could not delete archive. " + e.getMessage());
			return -1;
		}
		return 1;
	}

	private void doDelete(String clipid, String reason) throws AdapterException {
		FPPool pool = null;
		try {
			pool = openPool();		

			if (FPClip.Exists(pool, clipid)) {
				if (reason != null && reason.length() != 0) {
					FPClip.AuditedDelete(pool, clipid, reason, FPLibraryConstants.FP_OPTION_DELETE_PRIVILEGED);
				} else {
					FPClip.Delete(pool, clipid);
				}
			}

		} catch (FPLibraryException e) {
			log.severe("Centera SDK DELETE Api FPLibraryException: " + e.getMessage());
			throw new AdapterException(e);
		}
		catch (Exception exc) {
			log.severe("Centera SDK Delete API Error: " + exc.getMessage());
			throw new AdapterException(exc);
		}finally {
			closePool(pool);
		}
	}

	public boolean verify(CenteraClip centeraClip) throws AdapterException {
		return exists(centeraClip.clipid);
	}

	public boolean exists(String clipid) throws AdapterException {
		if (clipid == null || clipid.length() == 0) return false;
		if (!clipid.matches("[0-9_a-zA-Z]+")) return false;

		FPPool fpPool = null;
		try {
			fpPool = openPool();
			if (FPClip.Exists(fpPool, clipid)) {
				return true;
			}
		} catch (FPLibraryException e) {
			log.severe("Centera SDK VIEW API Error: (" + clipid + ") " + e.getMessage());
			throw new AdapterException(e);
		} finally {
			closePool(fpPool);
		}
		return false;
	}

	private FPPool openPool() throws FPLibraryException {
		return openPool(false);
	}

	private FPPool openPool(boolean query) throws FPLibraryException {

		FPPool.RegisterApplication(appName, appVersion);

		FPPool.setGlobalOption(
				FPLibraryConstants.FP_OPTION_OPENSTRATEGY, 
				FPLibraryConstants.FP_LAZY_OPEN);
		
		log.info("Thread:" + Thread.currentThread().getName()
				+ "; Opening Pool: " + casPoolInfo);

		FPPool pool = new FPPool(casPoolInfo);

		if (query && pool.getCapability(
				FPLibraryConstants.FP_CLIPENUMERATION,
				FPLibraryConstants.FP_ALLOWED) == "False") {
			throw new IllegalArgumentException("Query is not supported for this pool connection.");
		}

		return pool;
	}

	private void closeOutputStream(OutputStream os) {
		try {
			if (os != null) os.close();
		} catch (IOException e) {
		}		
	}

	private void closeInputStream(InputStream is) {
		try {
			if (is != null) is.close();
		} catch (IOException e) {
		}		
	}	

	private void closeClip(FPClip clip) {
		try {
			if (clip != null) clip.Close();
		} catch (FPLibraryException e) {
		}
	}

	private void closeTag(FPTag tag) {
		try {
			if (tag != null) tag.Close();
		} catch (FPLibraryException e) {
		}
	}

	private void closeQueryResult(FPQueryResult queryResult) {
		try {
			if (queryResult != null) queryResult.Close();
		} catch (FPLibraryException e) {
		}	
	}

	private void closePoolQuery(FPPoolQuery poolQuery) {
		try {
			if (poolQuery != null) poolQuery.Close();
		} catch (FPLibraryException e) {
		}	
	}

	private void closeQueryExpression(FPQueryExpression queryExp) {
		try {
			if (queryExp != null) queryExp.Close();
		} catch (FPLibraryException e) {
		}	
	}

	private void closePool(FPPool pool) {
		try {
			if (pool != null) pool.Close();
			log.info("Thread:" + Thread.currentThread().getName()
					+ "; Closing Pool: " + pool);
		} catch (FPLibraryException e) {
		}	
	}

}