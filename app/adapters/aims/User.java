package adapters.aims;

import java.util.List;

public class User {
	
	
	private String userName;
	private String firstName;
	private String lastName;	
	private List<AimsProject> projects;
	

	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public List<AimsProject> getProjects() {
		return projects;
	}
	public void setProjects(List<AimsProject> projects) {
		this.projects = projects;
	}	
	
}
