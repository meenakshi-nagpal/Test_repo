package compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.compress.compressors.CompressorException;

public class CompressionUtility {
		
	public int compress(InputStream rawIn,byte[] b,int off,int len) throws IOException,CompressorException{

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gzippedOut = new GZIPOutputStream(bos);
		
		byte[] buffer = new byte[len];
		int bytesRead=rawIn.read(buffer,off,len);
		if(bytesRead!=-1){
			gzippedOut.write(buffer,0,bytesRead);
			gzippedOut.close();
			byte[] bufferOut = bos.toByteArray();
			rawIn = new ByteArrayInputStream(bufferOut);
			int compressSize= rawIn.read(b,off,bufferOut.length);
			return compressSize;
		}
		return -1;
		
		
	}

	public static void decompress(OutputStream compressedOut,FileInputStream fin) throws IOException{
		
			try{
				GZIPInputStream gzippedIn = new GZIPInputStream(fin);
				byte[] buffer = new byte[16384];
				
				int bytesRead;
				while((bytesRead=gzippedIn.read(buffer))>0){
					compressedOut.write(buffer,0,bytesRead);
				}
				gzippedIn.close();
				compressedOut.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}

	}
	
}
