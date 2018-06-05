package adapters.file;

import java.io.File;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import utilities.Utility;
import adapters.AdapterException;

public class FileAdapter {

	private final Path dataPath;
	
	public FileAdapter(String projectId, String dir) {
		if (dir != null && dir.startsWith("file://")) {
			dataPath = Paths.get(URI.create(dir));
		} else {
			dataPath = Paths.get(dir);
		}
	}
	
	// public LocalFile put(LocalFile file, String projectid)  throws AdapterException {
 //    	Path source = file.getFile().toPath();
 //    	try {
 //    		Path newPath = dataPath.resolve(file.fileName);
 //    		Files.move(source, newPath, StandardCopyOption.REPLACE_EXISTING);
 //    		return new LocalFile(newPath.toFile());
 //    	} catch (IOException e) {
 //    		throw new AdapterException(e);
 //    	}
	// }

	public List<LocalFile> list(long offset, long length, String projectid) {
		if (Files.notExists(dataPath)) return Collections.emptyList();

		List<LocalFile> fileList = new ArrayList<LocalFile>();

		File[] files = dataPath.toFile().listFiles();
		for (File file : files) {
			LocalFile archiveFile = new LocalFile(file);
			fileList.add(archiveFile);
		}
		return fileList;
	}

	public LocalFile get(String id, String projectid) {
		return findFile(id, projectid);
	}

	// public void delete(String id, String projectid) throws AdapterException {
	// 	LocalFile file = findFile(id, projectid);
	// 	if (file != null) {
	// 		try {
	// 			Files.delete(file.getFile().toPath());
	// 		} catch (IOException e) {
	// 			throw new AdapterException(e);
	// 		}
	// 	}
	// }
	
	private  LocalFile findFile(String id, String projectid){
		for(LocalFile file : list(1, Integer.MAX_VALUE, projectid)){
			if (id.equals(file.guid)) return file;
		}		
		return null;
	}

	public String saveFile(String fileName, String data, String projectid)
	{
		if (Utility.isNullOrEmpty(data)) return "";

		String filePath = getPathString(fileName, projectid);

		if ("".equals(filePath)) return "";

		try {
		    Files.write(Paths.get(filePath), data.getBytes("utf-8"));

		} catch ( IOException e ) {
	        e.printStackTrace();
	        return "";
	    }
		return Paths.get(filePath).toUri().toString(); //filePath;
	}

	public String moveFile(String filename, File file, String projectid) {
		Path source = file.toPath();
		Path target = Paths.get(getPathString(filename, projectid));
		try {
			target = Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
			target = null;
		}
		return (target != null ? target.toUri().toString() : "");
	}

	private String getPathString(String fileName, String projectid) {
		String filePath = "";		
		
		if(dataPath == null)
			return "";
		if (Utility.isNullOrEmpty(fileName) || Utility.isNullOrEmpty(projectid)) 
			return "";
			
		String rootPath = dataPath.toString() + projectid; 
		try
		{
			System.out.println(Paths.get(rootPath).getParent().toString());
			//checking existence of parent of root
			if(!Files.isDirectory(Paths.get(rootPath).getParent()))
				return "";
			
			//checking existence of root with proj id
			if(!Files.isDirectory(Paths.get(rootPath)))
				Files.createDirectory(Paths.get(rootPath));
			
			String containerDirPath = rootPath + File.separator + utilities.Constant.EXTENDED_METADATA_CONTAINER_DIR;
			if (!Files.isDirectory(Paths.get(containerDirPath))) {
				Files.createDirectory(Paths.get(containerDirPath));
			}
			
			String yearPath = containerDirPath + File.separator + Utility.getCuurentYear();

			//File yearDir = new File(yearPath);
			if (!Files.isDirectory(Paths.get(yearPath))) {
				Files.createDirectory(Paths.get(yearPath));
			}
	
			String monthPath = yearPath + File.separator + Utility.getCuurentMonth();

			if (!Files.isDirectory(Paths.get(monthPath))) {
				Files.createDirectory(Paths.get(monthPath));
			}
			
			String dayPath = monthPath + File.separator + Utility.getCuurentDay();
			
			if (!Files.isDirectory(Paths.get(dayPath))) {
				Files.createDirectory(Paths.get(dayPath));
			}
			
			filePath = dayPath + File.separator + fileName;
		} catch ( IOException e ) {
	           e.printStackTrace();
	           return "";
	    }
		return filePath;		
	}
}
