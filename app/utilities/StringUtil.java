package utilities;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

public final class StringUtil {

	private final static Pattern basicAuthPattern = Pattern.compile("^(Basic) (.+)$", Pattern.CASE_INSENSITIVE);
	private final static Pattern ldapPattern = Pattern.compile("^(LDAP) (.+):(.+)$", Pattern.CASE_INSENSITIVE);

	private StringUtil() {}

	public static String encodeToBase64(String input) {
		byte[] inputs = input.getBytes();
		byte[] outputs = Base64.encodeBase64(inputs);
		return new String(outputs);
	}
	
	public static String decodeFromBase64(String input) {
		byte[] inputs = input.getBytes();
		byte[] outputs = Base64.decodeBase64(inputs);
		return new String(outputs);
	}

	public static String isBasicAuth(String authorizationHeader) {		
		if (authorizationHeader == null || authorizationHeader.length() == 0) return "";
		
		Matcher matcher = basicAuthPattern.matcher(authorizationHeader);
		System.out.println("matcher  "+matcher.matches());
		return matcher.matches() ? matcher.group(2) : "";	
	}

	public static Map decodeBasicAuth(String basicAuthString) {
		if (basicAuthString == null || basicAuthString.length() == 0) return Collections.emptyMap();

		String[] credString = decodeFromBase64(basicAuthString).split(":");
		if (credString == null || credString.length != 2) return Collections.emptyMap();
		
		String username = credString[0];
		String password = credString[1];

		Map<String, String> map = new HashMap<String, String>();
		map.put("username", username);
		map.put("password", password);
		return map;
	}

	public static Map decodeLdap(String authorizationHeader) {		
		if (authorizationHeader == null || authorizationHeader.length() == 0) return Collections.emptyMap();
		
		Matcher matcher = ldapPattern.matcher(authorizationHeader);
		if (!matcher.matches())  return Collections.emptyMap();
		
		if (!"LDAP".equalsIgnoreCase(matcher.group(1))) return Collections.emptyMap();
		
		String username = matcher.group(2);
		String password = matcher.group(3);
		System.out.println("username  "+username);
		System.out.println("password  "+password);
		Map<String, String> map = new HashMap<String, String>();
		map.put("username", username);
		map.put("password", password);
		return map;
	}

	public static String md5(String data) {
		return DigestUtils.md5Hex(data);
	}

}