package adapters.ldap;

import java.util.Hashtable;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LdapAdapter {

	private static final Logger log = Logger.getLogger(LdapAdapter.class.getName());
	
	 public static boolean isAuthenticated(String usr, String p, String url,
			 String baseDN, String userId,String password, 
			 String filter, String attributes, String factory,
			 String timeout) {
				 
		 	
		 
			DirContext ctx, ctx2 = null;
		 	boolean isAuthenticated =false;

		 	Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, factory);
			env.put(Context.PROVIDER_URL, url + "/" + baseDN);
			env.put(Context.REFERRAL, "follow");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, userId);
			env.put(Context.SECURITY_CREDENTIALS, password);
			
			try {
				//connect with LDAP server
				log.fine("Connecting to ldap instance ["+  url + "/" + baseDN + "]\n");
				ctx = new InitialDirContext(env);
			} catch (Exception e) {	
				log.severe("Failed to connect to ldap instance ["
						+    url + "/" + baseDN   + "] \n "+ e );
				return false;
			}

			
			NamingEnumeration<SearchResult> results = null;

			try {
				//search user attribute by binind user pwd
				SearchControls controls = new SearchControls();
				controls.setSearchScope(SearchControls.SUBTREE_SCOPE); 
				controls.setCountLimit(1); 
				controls.setTimeLimit(Integer.valueOf(timeout)); 
				String searchString = filter + usr + "))";

				results = ctx.search("", searchString, controls);

				if (results.hasMore()) {

					SearchResult result = (SearchResult) results.next();
					Attributes attrs = result.getAttributes();
					Attribute dnAttr = attrs.get(attributes);
					String dn = (String) dnAttr.get();
					log.fine("LDAP returned distinguishedName : " + dn);

					env.put(Context.SECURITY_PRINCIPAL, dn);
					env.put(Context.SECURITY_CREDENTIALS, p);

					ctx2 = new InitialDirContext(env); // Exception will be thrown on
					return true;
					//return ok(views.html.ilmsuccess.render("User authenticated successfully"));
				}
			} catch (AuthenticationException e) { 
				
				// Invalid Login
				log.severe("AuthenticationException: " + e);
				
			} catch (NameNotFoundException e) { 
				
				// The base context was not found.
				log.severe("NameNotFoundException: " + e);
				
			} catch (SizeLimitExceededException e) {
				
				//		"LDAP Query Limit Exceeded, adjust the query to bring back less records",
				log.severe("SizeLimitExceededException: " + e);
				
				
			} catch (NamingException e) {
				
				log.severe("NamingException: " + e);
				
			} finally {

				if (results != null) {
					
					try {
						
						results.close();
						
					} catch (Exception e) {
						
						log.severe("LDAP Search exception: " + e);
					}
				}
				
				if(ctx != null) {
					try {
						ctx.close();
					} catch (NamingException e) {
						log.severe("LDAP Resource exception: " + e);
						e.printStackTrace();
					}
				}
				
				if(ctx2 != null) {
					try {
						ctx2.close();
					} catch (NamingException e) {
						log.severe("LDAP Resource exception: " + e);
						e.printStackTrace();
					}
				}
			}

			return false;
	    }
}
