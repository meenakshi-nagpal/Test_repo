package models.storageapp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.cache.ServerCacheManager;

import play.Logger;
import play.db.ebean.Model;

@Entity
@Table(name = "APP_CONFIG")
public class AppConfigProperty extends Model {

	private static final long serialVersionUID = -2862346229839148617L;

	@Id
	@Column(name = "KEY")
	private String key;

	@Column(name = "VALUE")
	private String value;

	@Column(name = "DESCRIPTION")
	private String description;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public static AppConfigProperty getPropertyByKey(String key) {
		// clear cache in case the data in changed externally
/*		ServerCacheManager serverCacheManager =
				Ebean.getServerCacheManager();
		serverCacheManager.clear(AppConfigProperty.class);*/
		AppConfigProperty appConfigProperty = 
				Ebean.find(AppConfigProperty.class, key);
		if(appConfigProperty != null) {
			//Logger.debug("Property Name: " + appConfigProperty.getKey() + 
			//		" Property Value: " + appConfigProperty.getValue());
		} else {
			Logger.debug("Property Name: " + key + 
					" is not set in the DB");

		}
		return appConfigProperty;
	}

	// Utility Methods
	public Integer getIntValue() {
		if(value == null) return null;
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public Long getLongValue() {
		if(value == null) return null;
		try {
			return Long.valueOf(value);
		} catch (NumberFormatException e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public Boolean getBooleanValue() {
		if(value == null) return null;
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public Byte getByteValue() {
		if(value == null) return null;
		try {
			return Byte.valueOf(value);
		} catch (NumberFormatException e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public Float getFloatValue() {
		if(value == null) return null;
		try {
			return Float.valueOf(value);
		} catch (NumberFormatException e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public Double getDoubleValue() {
		if(value == null) return null;
		try {
			return Double.valueOf(value);
		} catch (NumberFormatException e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}

	public static String getAppConfigValue(String key) {
		AppConfigProperty appConfig = AppConfigProperty.getPropertyByKey(key);
		if (appConfig == null
				|| appConfig.getValue() == null
				|| appConfig.getValue()
				.trim().isEmpty())
			return "";
		else
			return 	appConfig.getValue().trim();
	}
	
	public static Integer getAppConfigIntValue(String key) {
		String val = getAppConfigValue(key);
		try {
			return Integer.valueOf(val);
		} catch(Exception e) {
			Logger.error("AppConfigProperty - parsing exception", e);
			e.printStackTrace();
		}
		return null;
	}
}
