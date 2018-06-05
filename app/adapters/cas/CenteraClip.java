package adapters.cas;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import com.filepool.fplibrary.FPLibraryConstants;

import adapters.AdapterException;

public class CenteraClip {

	public static final long DEFAULT_RETENTION_MATURITY = 0L;
	public static final long DEFAULT_RECORD_CODE_LENGTH = 60L;
	public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
	public static final String REPLACE_PATTERN = "[^.a-zA-Z0-9_-]|^[^a-zA-Z]+";

    public static String validTagname(String filename) {
        if (filename != null) {
            return filename.replaceAll(REPLACE_PATTERN, "_");
        } else return filename;
    }

    private static String DEFAULT = "null";
    
    private long retentionPeriod = 0L;
	public String clipid = DEFAULT;
	public String guid = DEFAULT;
	public String projectid = "0";
	public String creationUser = DEFAULT;
	public String profile = DEFAULT;
	public String server = "";
	public String filename = DEFAULT;
	private InputStream inputStream = null;
	//private File filepath=null;
	
	public long size = 0L;
	public long creationDate = 0L;
	public long modificationDate = 0L;
	public Boolean modified = Boolean.FALSE;
	public Boolean eventBasedRetention = Boolean.FALSE;
	public Boolean retentionHold = Boolean.FALSE;
	public String retentionClassName = DEFAULT;
	long maturityDate = 0L;
	public long recordCodePeriod = 0L;
	public String recordCode = DEFAULT;
	public String country = DEFAULT;
	public Map<String, String> attributes = Collections.emptyMap();

	public long ingestionStart = 0L;
	public long ingestionEnd = 0L;
	public String ait = DEFAULT;
	public String sourceServer = DEFAULT;
	public String sourceNamespace = DEFAULT;
	public String userAgent = DEFAULT;

	private String tagname = DEFAULT;
	private long retentionStart = 0L;
	private long retentionEnd = 0L;
	private String storageUri = DEFAULT;
	public String compressionEnabled;
	public long preCompressedSize=0L;
	

	private CenteraClip() {}
	
	public CenteraClip(String clipid) {
		this.clipid = clipid;
	}
	
	public CenteraClip(long maturityDate, long recordCodePeriod, String recordCode, String country) {
		setRetentionPeriod(maturityDate, recordCodePeriod);
		this.recordCode = recordCode;
		this.country = country;
	}

	public String getStorageUri() {
		return clipid;
	}
	
	public long getRetentionPeriod() {
		return retentionPeriod;
	}
	public void setRetentionPeriod(long retentionPeriod) {
		this.retentionPeriod = retentionPeriod;
	}
	public void setRetentionPeriod(long maturityDate, long recordCodePeriod) {
		this.recordCodePeriod = recordCodePeriod;
		long currentTime = System.currentTimeMillis();
		if (maturityDate <= 0) {
			this.maturityDate = currentTime;
		} else {
			this.maturityDate = maturityDate;
		}
		
		if(recordCodePeriod<0L){
			setRetentionPeriod(FPLibraryConstants.FP_INFINITE_RETENTION_PERIOD);
		}
		else{
			
			long remain = getRetentionEnd() - currentTime;
	
			if (remain <= 0L) {
				setRetentionPeriod(0L);
			} else {
				setRetentionPeriod(remain / 1000L);
			}
		}
	}
	public long getRetentionStart() {
		return maturityDate;
	}
	public long getMaturityDate() {
		return maturityDate;
	}

	public long getRetentionEnd() {
		if(retentionPeriod==FPLibraryConstants.FP_INFINITE_RETENTION_PERIOD){ 
			return -1L;
		}else{
			return getRetentionStart() + recordCodePeriod * 1000L;	
		}
	}
	

	
	
	public static Path getOutputPath(String clipid,char isCompressed, CenteraAdapter adapter) throws AdapterException {
        OutputStream  out = null;
        try {
            if (adapter.exists(clipid)) {
                Path path = Files.createTempFile(null, ".cas");
                out = new BufferedOutputStream(Files.newOutputStream(path));   
                adapter.read(clipid,isCompressed, out);
            
                return path;               
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new AdapterException(e);
        } finally {
            try {
                if (out != null) out.close();
            } catch (IOException e) {
            }
        }
	}

	public static File getOutputFile(String clipid,char isCompressed, CenteraAdapter adapter) throws AdapterException {
		Path path = getOutputPath(clipid, isCompressed, adapter);
		if (path == null) {
			return null;
		} else {
			return path.toFile();
		}
	}

	public void setInputFile(File file) throws IOException {
		setInputPath(file.toPath());
	}

	public void setInputPath(Path path) throws IOException {		
		inputStream = new BufferedInputStream(Files.newInputStream(path));
	}

	public void setInputStream(InputStream inputStream) {
		this.inputStream = inputStream;
	}
	public InputStream getInputStream() {
		return inputStream;
	}
	/**
	public void setFilepath(Path localPath) {
		if(localPath!=null) this.filepath = localPath.toFile();
	}
	public File getFilepath() {
		return this.filepath;
	}
	*/
	public String getTagname(){
		return validTagname(filename);
	}
	
	public String getCompressionEnabled() {
		if(compressionEnabled==null){
			return "false";
		}
		return compressionEnabled;
	}

	public void setCompressionEnabled(String compressionEnabled) {
		this.compressionEnabled = compressionEnabled;
	}
	
	



	
	@Override
	public String toString() {
		return clipid;
	}
	
	public String toStringExtended() {
		return "CenteraClip [retentionPeriod=" + retentionPeriod + ", clipid="
				+ clipid + ", guid=" + guid + ", projectid=" + projectid
				+ ", creationUser=" + creationUser + ", profile=" + profile
				+ ", server=" + server + ", filename=" + filename
				+ ", inputStream=" + inputStream + ", size=" + size
				+ ", creationDate=" + creationDate + ", modificationDate="
				+ modificationDate + ", modified=" + modified
				+ ", eventBasedRetention=" + eventBasedRetention
				+ ", retentionHold=" + retentionHold + ", retentionClassName="
				+ retentionClassName + ", maturityDate=" + maturityDate
				+ ", recordCodePeriod=" + recordCodePeriod + ", recordCode="
				+ recordCode + ", country=" + country + ", attributes="
				+ attributes + ", ingestionStart=" + ingestionStart
				+ ", ingestionEnd=" + ingestionEnd + ", ait=" + ait
				+ ", sourceServer=" + sourceServer + ", sourceNamespace="
				+ sourceNamespace + ", userAgent=" + userAgent + ", tagname="
				+ tagname + ", retentionStart=" + retentionStart
				+ ", retentionEnd=" + retentionEnd + ", storageUri="
				+ storageUri + "]";
	}
	
	
	
}
