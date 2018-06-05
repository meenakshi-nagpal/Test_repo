package utilities;

import javax.mail.internet.ContentDisposition;
import javax.mail.internet.ParseException;

public class MimeUtil {
	
	// getFilenameFromContentDispositionHeader method handles raw filenames passed in and assumes that any parser failures are likely a raw filename in the Content-Disposition header
	//
	// @param Header Content-Disposition header to parse
	// @return Filename extracted from header
	//
	// @see javax.mail.internet.ContentDisposition
	public static String getFilenameFromContentDispositionHeader(String Header) {
		try {
			ContentDisposition cd = new ContentDisposition(Header);
            String Filename = cd.getParameter("filename");
            if(Filename == null)
            	return Header;
            return Filename;
		} catch (ParseException e) {
            return Header;
		}
	}

}
