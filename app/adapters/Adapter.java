package adapters;

import java.io.OutputStream;
import java.util.List;

/**
 * Interface defines the methods for Adapter classes
 * @param <T> T is a Model Template represents a resource this adapter operates on
 */
public interface Adapter<T> {

	/**
	 * Used to insert or update represented by this adapter's Model Template
	 * @param t is a Model Template represents a resource
	 * @param projectid project id, put data only for this project if it is not blank
	 * @return T is a post put resource represented by this adapter's Model Template
	 */
	public  T put(T t, String projectid) throws AdapterException;

	/**
	 * List out a list of resources represented by this Model Template
	 * input parameters' usage would be defined by implemntation class
	 * @param start inclusive, could be used as a position in a sequence or as a timestamp
	 * @param range exclusive, could be used as a position in a sequence or as a timestamp
	 * @param projectid project id, return data only for this project if it is not blank	 
	 * @return List a list of resources represented by this adapter's Model Template
	 */
	public  List<T> list(long start, long range, String projectid) throws AdapterException;

	/**
	 * Retrieve a resource represented by this adapter's Model Template
	 * @param id Identifier of a resource
	 * @param projectid project id, return data only for this project if it is not blank
	 * @return a resource encapsulated in this adapter's Model Template
	 */
	public  T get(String id, String projectid) throws AdapterException;

	/**
	 * Delete a resource by its id
	 * @param projectid project id, delete data only for this project if it is not blank
	 * @param id Identifier of a resource
	 */
	public  void delete(String id, String projectid) throws AdapterException;

	/**
	 * Streaming a resource
	 * @param id Identifier of resource
	 * @param projectid project id, return data only for this project if it is not blank
	 * @param outputStream  The OutPutStream object in which this resource is to be written into.
	 */
	public  void read(String id, OutputStream outputStream, String projectid) throws AdapterException;
	
	
	/**
	 * Verify the validity of a resource 
	 * @param id Identifier of a resource
	 * @param projectid project id, verify data only for this project if it is not blank
	 * @return Boolean true if this resource is valid, otherwise false
	 */
	public boolean verify(T t, String projectid) throws AdapterException;
}
