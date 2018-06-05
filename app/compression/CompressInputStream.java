/**
 * 
 */
package compression;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.compressors.CompressorException;

/**
 * @author ZKXKF47
 *
 */
public class CompressInputStream extends InputStream {
	
	InputStream rawIn;
	CompressionUtility compression=null;
	public CompressInputStream(InputStream in){
		rawIn=in;
		compression=new CompressionUtility();
	}

	/* (non-Javadoc)
	 * @see java.io.InputStream#read()
	 */
	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int read(byte[] b) throws IOException {
		try {
			
			return compression.compress(rawIn,b,0,b.length);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Exception Occurred while compressing");
		}
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		try {
			return compression.compress(rawIn,b,off,len);
		} catch (CompressorException e) {
			e.printStackTrace();
			throw new IOException("Exception Occurred while compressing");
		}
		
	}

	@Override
	public void close() throws IOException {
		try{
			if(rawIn!=null){
				rawIn.close();
			}
		}catch(IOException e){
			
		}
		super.close();
	}
	
	
	
	

}
