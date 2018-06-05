package adapters.aims;

import java.util.logging.Logger;

import org.apache.jcs.JCS;

import org.apache.jcs.access.exception.CacheException;

public class AimsCache {

	private static final Logger log = Logger.getLogger(AimsCache.class
			.getName());
	private JCS cache;

	public AimsCache() {
		try {
			// Load the cache
			cache = JCS.getInstance("projectCache");
		} catch (CacheException e) {

			log.severe("Error initializing cache: " + e);

		}

	}

	public void addProject(AimsProject project) {
		try {

			java.util.Date date = new java.util.Date();

			project.setCacheTimestamp(new java.util.Date());
			cache.put(project.getId(), project);
		} catch (CacheException e) {
			log.severe("Error adding project to cache: " + e);
		}
	}

	public AimsProject getProject(String id) {
		return (AimsProject) cache.get(id);
	}

	public void removeProject(String id) {
		try {
			cache.remove(id);
		} catch (CacheException e) {
			log.severe("Error removing project from cache: " + e);
		}
	}

}
