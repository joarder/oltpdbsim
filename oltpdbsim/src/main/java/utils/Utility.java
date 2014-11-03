package main.java.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import main.java.entry.Global;

import org.apache.commons.codec.digest.DigestUtils;

public class Utility {
	
	@SuppressWarnings("unused")
	private static double exp(double mean) {
        return -mean * Math.log(Global.rand.nextDouble());
    }
	
	public static double round(double value, int places) {
		if(places < 0) throw new IllegalArgumentException();
		
		long factor = (long) Math.pow(10, places);
		value *= factor;
		long temp = Math.round(value);
		
		return (double) temp/factor;
	}
	
	// Returns a simple hash key
	public static int simpleHash(int x, int divisor) {
		//Global.LOGGER.info("@debug >> x = "+x+" divisor = "+divisor);
		return ((x % divisor) + 1);
	}	
	
	public static int convertByteToInt(byte[] b) {           
	    int value= 0;
	    
	    for(int i = 0; i < b.length; i++)
	       value = (value << 8) | b[i];
	    
	    return value;       
	}
	
	public static long convertByteToUnsignedLong(byte[] b) {	
		return (ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong() & 0xFFFFFFFFL);
	}
	
	// Returns a Java hash value
	public static long javaHash(String key) {
		  return key.hashCode();
	}	
	
	// Returns a MD5 hash value
	public static long md5Hash(String key) {
		  byte[] value = DigestUtils.md5(key.getBytes());
		  return Utility.convertByteToUnsignedLong(value);
	}
	
	// Returns a SHA1 hash value
	public static long sha1Hash(String key) {
		  byte[] value = DigestUtils.sha1(key.getBytes());
		  return Utility.convertByteToUnsignedLong(value);
	}	
	
	// Add padding value in the least significant bits of id
	public static int rightPadding(int id, int value) {		
		return ((int) Math.pow(10, Math.floor(Math.log10(value)) + 1) * id + value);
	}	

	public static String asUnsignedDecimalString(long l) {
		/** the constant 2^64 */
		BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);
		BigInteger b = BigInteger.valueOf(l);
	   
		if(b.signum() < 0) {
			b = b.add(TWO_64);
		}
	   
		return b.toString();
	}
	
	public static boolean inRange(long min, long max, long x) {
		return (x >= min && x <= max);		
	}
	
	public static PrintWriter getPrintWriter(String dir, String file_name) {		
		File file = new File(dir+file_name+".txt");
		PrintWriter prWriter = null;
		
		try {
			file.getParentFile().mkdirs();
			file.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(file)));				
			} catch(IOException e) {
				Global.LOGGER.error("Failed to create a print writer !!", e);
			}
		} catch (IOException e) {		
			Global.LOGGER.error("Failed to create a file !!", e);
		}
		
		return prWriter;
	}
	
	public static PrintWriter getPrintWriter(String dir, File file) {		
		PrintWriter prWriter = null;
		
		try {
			file.getParentFile().mkdirs();
			file.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(file)));				
			} catch(IOException e) {
				Global.LOGGER.error("Failed to create a print writer !!", e);
			}
		} catch (IOException e) {		
			Global.LOGGER.error("Failed to create a file !!", e);
		}
		
		return prWriter;
	}
	
	public static boolean isWindows() {
		return (Global.OS.indexOf("win") >= 0);
	}
 
	public static boolean isUnix() {
		return (Global.OS.indexOf("nix") >= 0 || Global.OS.indexOf("nux") >= 0 || Global.OS.indexOf("aix") > 0 );
	}
}	