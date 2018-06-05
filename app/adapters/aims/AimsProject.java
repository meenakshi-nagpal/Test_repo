package adapters.aims;

import java.io.Serializable;
import java.util.Date; 


public class AimsProject  implements Serializable{

	private String id;
	private String name;
	public Ait ait = Ait.DEFAULT;
	public RecordCode recordCode = RecordCode.DEFAULT;
	private Date  cacheTimestamp;
	
	public String getId() {
		return id;
	}
	public Date getCacheTimestamp() {
		return cacheTimestamp;
	}
	public void setCacheTimestamp(Date cacheTimestamp) {
		this.cacheTimestamp = cacheTimestamp;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
