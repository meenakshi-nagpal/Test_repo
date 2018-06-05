package utilities;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;

public final class Guid {



        public static String guid(){
            return guid(digit(3), "");
        } 

        public static String guid(String projectid){
            return guid(projectid, "-"); 
        }

        private static String guid(String prefix1, String prefix2){
            StringBuilder guid = new StringBuilder();
            
            guid.append(prefix1).append(prefix2).append(UUID.randomUUID().toString().replaceAll("-", ""));
            
            return guid.toString();
        }
        
        public static String assignFilename(String projectid, String username, long contentLength){
            StringBuilder filename = new StringBuilder();
            String proj = "0";
            if (projectid != null && projectid.length() != 0) proj = projectid;
            
            String name = "username";
            if (username != null && username.length() != 0) name = username;
            
            filename.append(proj)
            .append("-")
            .append(name)
            .append("-")
            .append(contentLength)
            .append("-")
            .append(System.currentTimeMillis());
            
            return filename.toString();
        }

        public static String digit(int len){
                        double rangeD = Math.pow(10D, (double)len);
                        long rangeL = (long)rangeD;
                        long ceil = rangeL - 1;
                        long floor = rangeL / 10L;
                        long digit = Math.round(Math.random() * rangeD);;
                        digit = Math.min(digit, ceil);
                        digit = Math.max(digit, floor);
                        return Long.toString(digit);
        }

        
        private static String host() {
                        String hostname = null;
                        try {
                                        hostname = java.net.InetAddress.getLocalHost().getHostName();
                        } catch (Exception e) {
                                        hostname = "localhost";
                        }
                        return Long.toHexString(hostname.hashCode());
        }

        public static String secureRandom(int len) {
                        SecureRandom sr;
                        try {
                                        sr = SecureRandom.getInstance("SHA1PRNG");
                                        byte[] bytes = new byte[len];
                                        sr.nextBytes(bytes);
                                        char[] chars = new char[bytes.length];
                                        for (int i=0; i<bytes.length; i++) {
                                                        chars[i] = (char)bytes[i];
                                        }
                                        return new String(chars);
                        } catch (NoSuchAlgorithmException e) {
                                        return digit(len);
                        }
        }              
}