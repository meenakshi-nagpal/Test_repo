package adapters.aims;

import java.io.Serializable;

public class Ait implements Serializable {
	final public static Ait DEFAULT = new Ait("null", "null");

	public String id;
	public String name;

	private Ait() {}

	public Ait(String id, String name) {
		this.id = id;
		this.name = name;
	}

	@Override
	public String toString() {
		return "{\"ait\": {\"id\": \"" + id + "\", \"name\": \"" + name + "\"}";
	}
}