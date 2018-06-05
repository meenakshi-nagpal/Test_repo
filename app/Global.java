import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import com.avaje.ebean.Ebean;

import controllers.JobController;
import play.Application;
import play.GlobalSettings;
import play.Play;
import play.mvc.Action;
import play.libs.F;
import play.libs.F.Promise;
import play.mvc.Http.Context;
import play.mvc.Http.Request;
import play.mvc.Http.RequestHeader;
import play.mvc.SimpleResult;
import play.api.mvc.EssentialFilter;
import play.cache.Cache;
import play.db.ebean.Model;
import play.filters.gzip.GzipFilter;
import play.Logger;
import adapters.aims.AimsAdapter;
import adapters.ldap.LdapAdapter;
import adapters.AdapterException;
import utilities.Constant;
import utilities.StringUtil;
import utilities.Guid;
import utilities.Utility;


import models.storageapp.AccessData;
import models.storageapp.AppConfigProperty;
import models.storageapp.Helper;
import models.storageapp.Storage;

public class Global extends GlobalSettings {
	@Override
	public void onStart(Application arg0) {
		try {

			// Multiple CAS Support
			//setupCacheForMultipleCAS();
			
			//multiple HCP and S3 configuration
			setupCacheForHcpAndS3OrCAS();
			
			JobController.startAllJobs();
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();
			
			Logger.info("All jobs atarted at " + new java.util.Date());
		} catch (SchedulerException e) {
			Logger.error("Global - exception occurred while scheduling jobs", e);
			e.printStackTrace();
		}
	}
	
    public <T extends EssentialFilter> Class<T>[] filters() {
        return new Class[]{GzipFilter.class};
    }

	@Override
	public Action onRequest(Request request, Method method) {
		final String remoteAddress = request.remoteAddress() + " - ";
		Logger.info(remoteAddress + request.toString() + " - " + method);
		String uri = request.uri();	
		Logger.info("uri- "+uri);
		boolean authenticated = false;
		boolean basicAuth = false;
		boolean checkAuthorization = true;
		if (uri.startsWith("/public/")) {
			Logger.info("uri.startsWith public return");
			return super.onRequest(request, method);
		} else if (uri.startsWith("/private/")) {
			Logger.info("uri.startsWith private check private auth");
			if (privateAuthe(request)) {
				authenticated = true;
				checkAuthorization = false;
			}
		} else {
			Logger.info("uri.startsWith non private/public");
			String auth = isBasicAuth(request); // checking BASIC header only and returns encrypted user/pwd
			Logger.info("uri.startsWith non private/public auth  "+auth);
			if ("".equals(auth) && appAuthe(request)) authenticated = true; // appAuthe verify through ldapif not basic
			else {
				Logger.info("do basic authentication  "+auth);
				if (doBasicAuth(request, auth)) authenticated = true;
				else basicAuth = true;
			}
		}

		String username = request.username();

		if (authenticated) {
			Logger.info(remoteAddress + username + " is authenticated");
		} else {
			Logger.info(remoteAddress + username + " is not authenticated");
			if (basicAuth) return BasicAuthAction.INSTANCE;
			else return Four01Action.INSTANCE;
		}

		if (checkAuthorization) {
			String projectid = getProjectid(uri);
			Logger.info("projectid: "+projectid);
			if ("".equals(projectid)) {
				Logger.info(remoteAddress + "invalid project ID");
				return BadRequest.INSTANCE;
			} 

			String role = Play.application().configuration().getString("role.aims.normal");
			
			String read_role = Play.application().configuration().getString("role.aims.read");
			
			String requestMethod = request.method();
			
			//String role_permissions =  Play.application().configuration().getString("role.aims.normal.permissions");
			
			//String read_role_permissions = Play.application().configuration().getString("role.aims.read.permissions");
			
			Boolean rbacresult = false;
			
			if(requestMethod.equals("GET")){
				Logger.info("Method Get");
				if(rbac(username, read_role, projectid,request) || rbac(username, role, projectid,request)){
					rbacresult = true;
				}
			}else if(requestMethod.equals("POST") || requestMethod.equals("PUT")){
				Logger.info("Method POST PUT");
				rbacresult = rbac(username, role, projectid,request);
			}
			
			
			if (!rbacresult) return Four01Action.INSTANCE;
		} else {
			Logger.info(remoteAddress + username + " is authorized by default");
		}
				
		return super.onRequest(request, method);
	}

	private boolean doBasicAuth(Request request, String basicAuthString) {
		Map<String, String> credMap = StringUtil.decodeBasicAuth(basicAuthString);
		if (credMap.isEmpty()) return false;
		
		String username = credMap.get("username");
		String password = credMap.get("password");

		request.setUsername(username);

		username = ldap(username, password);
		if ("".equals(username)) return false;

		request.setUsername(username);
		return true;
	}

	private String isBasicAuth(Request request) {	
		Logger.info("request.getHeader  "+request.getHeader("Authorization"));
		String s = StringUtil.isBasicAuth(request.getHeader("Authorization"));
		Logger.info("s:    "+s);
		return 	s;
	}

	private boolean appAuthe(Request request) {
		final String authHeader = request.getHeader("Authorization");
		Logger.info("appAuthe authHeader"+authHeader);
		Map<String, String> map = StringUtil.decodeLdap(authHeader);
		if (map.isEmpty()) return false;

		String username = map.get("username");
		String password = map.get("password");

		request.setUsername(username);
		Logger.info("authenticating through LDAP");
		username = ldap(username, password);
		if ("".equals(username)) return false;
				
		request.setUsername(username);
		return true;
	}

	private String ldap(String username, String password) {
		boolean isAuthenticated = false;
		
		String user="";
		String domain="";

		if (username == null || username.length() == 0) return "";

		int exist = username.indexOf("\\");
		if (exist == -1) {
			user = username;
		} else {
			StringTokenizer st = new StringTokenizer(username, "\\");
			 if(st.hasMoreTokens()) {
				 domain = st.nextToken();
		     }if(st.hasMoreTokens()) {
		         user = st.nextToken();
		     }
		}
				
		String ldapUrl = Play.application().configuration().getString("ldap.url");
		String ldapBaseDn = Play.application().configuration().getString("ldap.baseDN");
		String ldapUserId = Play.application().configuration().getString("ldap.userId");
		String ldapPassword = Play.application().configuration().getString("ldap.password");
		String ldapFilter = Play.application().configuration().getString("ldap.filter");
		String ldapAttributes = Play.application().configuration().getString("ldap.attributes");
		String ldapFactory = Play.application().configuration().getString("ldap.factory");
		String ldapmlurl = Play.application().configuration().getString("ldap.mlurl");
		String mlbaseDN = Play.application().configuration().getString("ldap.mlbaseDN");
		String mluserId = Play.application().configuration().getString("ldap.mluserId");
		String timeout = Play.application().configuration().getString("ldap.timeout");
		
		if("amrs".equalsIgnoreCase(domain)){
			isAuthenticated = LdapAdapter.isAuthenticated(user, password, 
					ldapmlurl,
					mlbaseDN,
					user+mluserId,
					password,
					ldapFilter,
					ldapAttributes,
					ldapFactory,
					timeout);
		} else {
			isAuthenticated = LdapAdapter.isAuthenticated(user, password, 
					ldapUrl,
					ldapBaseDn,
					ldapUserId,
					ldapPassword,
					ldapFilter,
					ldapAttributes,
					ldapFactory,
					timeout);
		}
		

		return (isAuthenticated ? user : "");
	}
	
	private boolean rbac(String username, String role, String projectid, Request request) {
		final String remoteAddress = request.remoteAddress() + " - ";
		boolean access = false;
		String read_role = Play.application().configuration().getString("role.aims.read");
		final long currentTime = System.currentTimeMillis();		
		AccessData accessData = new AccessData();
		AimsAdapter aimsAdapter = AimsAdapter.getInstance();
		try {
			access = aimsAdapter.authorizeUser(username, role, projectid);
		} catch (AdapterException e) {
			Logger.error(remoteAddress + "Failed to authorize user by AimsAdapter: " + e +" now authorizing from history access data.");
			try{
				access = accessData.authorizeFromAccessData(username, role, projectid);
			}catch(Exception ex){
				Logger.error("Exception- "+ex);
			}
			
		}

		String requestMethod = request.method();
		
		String uri = request.uri();
		
		if (access) {
			Logger.info(remoteAddress + username + " is authorized for role(" + role + ") to project(" + projectid + ")");
			try{
				if((requestMethod.equals("GET")&& uri.contains("download"))||(!requestMethod.equals("GET"))){
					accessData.setId(Guid.guid());
					accessData.setUserId(username);
					accessData.setRole(role);
					accessData.setProjectId(projectid);
					accessData.setIsSuccessfull(1);
					accessData.setUri(requestMethod + " " + uri);
					accessData.setAccessTimeStamp(currentTime);
					accessData.save();
				}
			}catch(Exception e){
				Logger.error(remoteAddress + "Exception- "+e);
			}
		} else {
			
			try{	
				if(!role.equals(read_role)){
					Logger.info(remoteAddress + username + " is not authorized for role(" + role + ") to project(" + projectid + ")");
					if((requestMethod.equals("GET") && uri.contains("download"))||(!requestMethod.equals("GET"))){
					
						accessData.setId(Guid.guid());
						accessData.setUserId(username);
						accessData.setRole(role);
						accessData.setProjectId(projectid);
						accessData.setIsSuccessfull(0);
						accessData.setUri(requestMethod + " " + uri);
						accessData.setAccessTimeStamp(currentTime);
						accessData.save();
					}
				}
			}catch(Exception e){
				Logger.error(remoteAddress + "Exception- "+e);
			}
		}
		return access;
	}
	
	
	private boolean privateAuthe(Request request) {		
		Logger.info("Entered privateAuth");
		Map<String, String> authnHeader = getAuthnHeader(request);
		if (authnHeader.isEmpty()) return false;

		String accessId = authnHeader.get("accessId");
		Logger.info("privateAuth accessId"+accessId);
		String localSig = getLocalSig(accessId);
		Logger.info("privateAuth localSig"+localSig);
		if ("".equalsIgnoreCase(localSig)) return false;

		if (!localSig.equals(authnHeader.get("signature"))) return false;

		request.setUsername(accessId);
		return true;
	}

	private String getLocalSig(String accessId) {
		String val = Play.application().configuration().getString(accessId + ".sig");
		Logger.info("getLocalSig "+val);
		return (val != null ? val : "");
	}		
	
	private Map<String, String> getAuthnHeader(RequestHeader header) {
		Logger.info("entered getAuthnHeader");
		final String remoteAddress = header.remoteAddress() + " - ";
		Logger.info("getAuthnHeader remoteAddress "+remoteAddress);
		final String authHeader = header.getHeader("Authorization");
		Logger.info("getAuthnHeader authHeader "+authHeader);
		if (authHeader == null || authHeader.length() == 0) {
			Logger.debug(remoteAddress + "Authorization header is blank.");
			return Collections.emptyMap();
		}
		
		Matcher matcher = authPattern.matcher(authHeader);
		Logger.info("getAuthnHeader matcher "+matcher);
		if (!matcher.matches())  return Collections.emptyMap();
		Logger.info("getAuthnHeader matcher.group(1) "+matcher.group(1));
		if (!"ILM".equalsIgnoreCase(matcher.group(1))) return Collections.emptyMap();
		
		String accessId = matcher.group(2);
		String sig = matcher.group(3);
		Logger.info("getAuthnHeader accessId: "+accessId);
		Logger.info("getAuthnHeader sig: "+sig);
		Map<String, String> authnHeader = new HashMap<String, String>();
		authnHeader.put("accessId", accessId);
		authnHeader.put("signature", sig);
		
		return authnHeader;
	}

	private String getProjectid(String uri) {
		Matcher matcher = projectidPattern.matcher(uri);
		if (!matcher.matches()) return "";
		Logger.info("getProjectid matcher.group(1): "+matcher.group(1));
		return matcher.group(1);
	}
	
	private final static Pattern authPattern = Pattern.compile("^(ILM) (.+):(.+)$", Pattern.CASE_INSENSITIVE);
	private final static Pattern projectidPattern = Pattern.compile(".+/projects/([0-9]+)($|(/|\\?)+.*)");
	
	private static class Four01Action extends Action.Simple {
		
		public final static Action INSTANCE = new Four01Action();

		@Override
		public Promise<SimpleResult> call(Context ctx) throws Throwable {
			return F.Promise.pure((SimpleResult) unauthorized("401 Unauthorized"));
		}
			
	}

	private static class BasicAuthAction extends Action.Simple {
		
		public final static Action INSTANCE = new BasicAuthAction();

		@Override
		public Promise<SimpleResult> call(Context ctx) throws Throwable {
			ctx.response().setHeader("WWW-Authenticate", "Basic realm=\"AD domains\"");
			return F.Promise.pure((SimpleResult) unauthorized("401 Unauthorized"));
		}
			
	}

	private static class BadRequest extends Action.Simple {
		
		public final static Action INSTANCE = new BadRequest();

		@Override
		public Promise<SimpleResult> call(Context ctx) throws Throwable {
			return F.Promise.pure((SimpleResult) badRequest("400 Bad Request"));
		}
			
	}
	
	/**
	private void setupCacheForMultipleCAS() {
		Integer CURRENT_WRITE_STORAGE_ID = Integer.valueOf(
				System.getProperty(Constant.CURRENT_WRITE_STORAGE_APP_PROPERTY));

		String poolString = Helper.getCASStorageConnectionString(CURRENT_WRITE_STORAGE_ID);
		
		Cache.set(Constant.CURRENT_WRITE_STORAGE_KEY, CURRENT_WRITE_STORAGE_ID);
		Cache.set(Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY, poolString);
		
	}
*/


	private void setupCacheForHcpAndS3OrCAS() {
	
     	Storage storage = Helper.getStorageConnectionDetails();
     	try{
     	if(storage == null){
     		throw new Exception();
     	}
       
     	Integer CURRENT_WRITE_STORAGE_ID = storage.getId();
     	Cache.set(Constant.CURRENT_WRITE_STORAGE_KEY, CURRENT_WRITE_STORAGE_ID);
		//Cache.set(Constant.HCP_CURRENT_WRITE_STORAGE_ID,CURRENT_WRITE_STORAGE_ID);
	    // Handle if there is failover to secondary storage, if storage_status=6(Failover)
				if(storage != null) {
					if(storage.getStorageStatus() == Constant.STORAGE_FAILOVER_STATUS) {
						storage = new Model.Finder<Integer, Storage>(
								Integer.class, Storage.class).byId(
										storage.getBackupStorageId());
						Logger.info("Loading Backup storage id to cache.");
					}
				}
				
				Logger.debug("Global setting up cache: storage: "+storage);
				Integer StorageTypeId=storage.getStorageType();
				Logger.info("Global setting up cache: storageType: "+StorageTypeId);
				Cache.set(Constant.CURRENT_WRITE_STORAGE_TYPE, StorageTypeId);
				//check the storage type, if 1 load for centera
				if(StorageTypeId==Constant.STORAGE_TYPE_CENTERA_ID){
					//Load Centera pool into cache
					String poolString = Helper.getCASStorageConnectionString(CURRENT_WRITE_STORAGE_ID);
					Cache.set(Constant.CURRENT_WRITE_STORAGE_POOL_INFO_KEY, poolString);
					
				}else if(StorageTypeId==Constant.STORAGE_TYPE_HCP_ID){
					//Load HCP bucket configuration into cache
					Cache.set(Constant.HCP_CURRENT_REST_END_URL, storage.getHcpRestUrl());
					Cache.set(Constant.HCP_CURRENT_S3_END_URL,  storage.getHcpS3Url());
				    Cache.set(Constant.HCP_CURRENT_BUCKET,  storage.getBucketName());
					Cache.set(Constant.HCP_CURRENT_ACCESS_KEY,storage.getAccessKey());
					Cache.set(Constant.HCP_CURRENT_SECRET_KEY, storage.getSecretKey());
					
				}
				
	
		
	}
		catch(Exception e){
			Logger.error("Exception Occured while setting up cache for HCP or Centera Cas pool"+e);
		}
	}
}
