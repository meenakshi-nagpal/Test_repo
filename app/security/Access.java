package security;

public interface Access {
	
	public boolean isAuthenticated(String standardid, String password);

	public boolean isAuthorized(String standardid, String resource, String role);
}