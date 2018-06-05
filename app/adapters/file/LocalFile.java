package adapters.file;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import utilities.StringUtil;

public class LocalFile {

	public String guid = "";
	public String name = "";
	public Long size = Long.MIN_VALUE;
	public boolean dir = false;
	private File file = null;
	private Path path = null;
	public String str = null;
	public String parent = null;

	public LocalFile(File file) {
		this(file, file.getName());
	}

	public LocalFile(File file, String name) {
		setFile(file);
		this.name = name;
		guid = Integer.toHexString(name.hashCode());
	}

	public File getFile(){
		return file;
	}

	private void setFile(File file) {
		this.file = file;
		dir = file.isDirectory();
		this.path = file.toPath();
		str = StringUtil.encodeToBase64(this.path.toString());
		parent = StringUtil.encodeToBase64(this.path.getParent().toString());
		try {
			BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
			size = attrs.size();
		} catch (IOException e) {
			size = -1L;	
		}
	}
}