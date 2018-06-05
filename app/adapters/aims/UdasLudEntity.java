package adapters.aims;

import java.sql.Date;


public class UdasLudEntity {
	
	private String user_id;
	private AIMSRoleMapping role;
	private Date lud;
	
	
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public Date getLud() {
		return lud;
	}
	public void setLud(Date lud) {
		this.lud = lud;
	}
	public AIMSRoleMapping getRole() {
		return role;
	}
	public void setRole(AIMSRoleMapping role) {
		this.role = role;
	}

}
