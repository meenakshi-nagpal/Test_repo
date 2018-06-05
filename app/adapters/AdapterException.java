package adapters;

public class AdapterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1041036777215729465L;
	
    /**
     * Creates a new AdapterException object.
     */
    public AdapterException() {
        super();
    }

    /**
     * Creates a new AdapterException object.
     *
     * @param e Exception
     */
    public AdapterException(Exception e) {
        super(e);
    }


    /**
     * Creates a new AdapterException object.
     *
     * @param msg Error message.  Displayed in the log.
     * @param e Exception
     */
    public AdapterException(String msg, Exception e) {
        super(msg, e);
    }

    /**
     * Creates a new AdapterException object.
     *
     * @param message Error message.  Displayed in the log.
     */
    public AdapterException(String message) {
        super(message);
    }

}
