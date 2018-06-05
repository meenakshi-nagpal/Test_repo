package adapters.aims;

public class AIMSConstants {
	
	public static final int READ_WRITE_ID = 9;
	public static final int READ_ID = 12;
	
	public static final String UDAS_LUD_UPDATE_QUERY =
			"UPDATE Entitlement_Staging " +
					"SET last_used_date = ?, " +
					"  Sent_To_CSDB     = 0 " +
					"WHERE Grantee      = ? " +
					"AND Permission     = ?";

}
