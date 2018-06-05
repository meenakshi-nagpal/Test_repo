package adapters.aims;


public enum AIMSRoleMapping {
	
	UDAS_WRITE(AIMSConstants.READ_WRITE_ID),
	UDAS_READ(AIMSConstants.READ_ID);
	
	private int roleId;
	
	private AIMSRoleMapping(int roleId) {
		this.roleId = roleId;
	}

	public int getRoleId() {
		return roleId;
	}
	
	public static AIMSRoleMapping getInstance(int roleId) {
		if(roleId == AIMSConstants.READ_WRITE_ID) return  UDAS_WRITE;
		
		if(roleId == AIMSConstants.READ_ID) return UDAS_READ;
		
		throw new IllegalArgumentException("Undefined UDAS AIMS Role Mapping.");
	}

}
