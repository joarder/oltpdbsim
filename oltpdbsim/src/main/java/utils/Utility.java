/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.TreeMap;

import main.java.entry.Global;

import org.apache.commons.codec.digest.DigestUtils;

public class Utility {

	private static final Random rand = new Random(); 
	private static final ThreadLocal<Random> rng = new ThreadLocal<Random>();

	public static Random random() {
		Random ret = rng.get();
	    
	    if(ret == null) {
	    	ret = new Random(rand.nextLong());
	    	rng.set(ret);
	    }
	    
	    return ret;
	}
	
	// Returns the Base
	public static int getBase(int x) { 
		int value = Math.abs(x);
		
		if (value == 0) 
			return 1; 
		else 
			return (int) (1 + Math.floor((Math.log(value)/Math.log(10.0d)))); 
	}
	
	// Randomly shuffles a given Array
	public static void shuffleArray(int[] array) {
	    int index, temp;	    
	    for (int i = array.length - 1; i > 1; i--)
	    {
	        index = Global.rand.nextInt(i-1) + 1;
	        temp = array[index];
	        array[index] = array[i];
	        array[i] = temp;
	    }
	}
	
	// Collected from http://rosettacode.org/wiki/Remove_lines_from_a_file#Java
	public static void deleteLinesFromFile(String filename, int startline, int numlines) {
		try {
			BufferedReader br=new BufferedReader(new FileReader(filename));
 
			//String buffer to store contents of the file
			StringBuffer sb=new StringBuffer("");
 
			//Keep track of the line number
			int linenumber=1;
			String line;
 
			while((line=br.readLine())!=null) {
				//Store each valid line in the string buffer
				if(linenumber<startline||linenumber>=startline+numlines)
					sb.append(line+"\n");
				linenumber++;
			}
			
			if(startline+numlines>linenumber)
				System.out.println("End of file reached.");
			br.close();
 
			FileWriter fw=new FileWriter(new File(filename));
			
			//Write entire string buffer into the file
			fw.write(sb.toString());
			fw.close();
			
		} catch (Exception e) {
			System.out.println("Something went horribly wrong: "+e.getMessage());
		}
	}
	
	// Returns a normalised value between a and b for the given x 
	public static double normalise(double min, double max, double x, double a, double b) {
		return (a + (((x - min) * (b - a))/(max - min)));
	}	
	
	// Used for creating 2D Matrix of size of total Partition numbers
	// Used in MethodX
	public static Matrix createMatrix(int M, int N) {		
		// Create a 2D Matrix
		MatrixElement[][] mapping = new MatrixElement[M][N];
		
		// Initialization
		int id = 0;
		for(int i = 0; i < M; i++) {
			for(int j = 0; j < N; j++) {
				if(i == j) {
					mapping[i][j] = new MatrixElement(++id, i, j, -1.0d);
					
				} else if(i == 0) {
					mapping[i][j] = new MatrixElement(++id, i, j, j);
					
				} else if(j == 0) {
					mapping[i][j] = new MatrixElement(++id, i, j, i);
					
				} else {
					mapping[i][j] = new MatrixElement(++id, i, j, 0.0d);
					mapping[j][i] = new MatrixElement(++id, j, i, -1.0d);					
				}
			}
		}
				
		// Create and return the Matrix
		return (new Matrix(mapping));
	}	
	
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
	
	// 
	public static String getSaltedKey(String key) {
		int prefix = (++Global.global_index % Global.partitions);
		Global.global_index_map.put(Integer.parseInt(key), prefix);
		
		return (Integer.toString(prefix) + key);
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
	
	// Knuth's Multiplication Method
	public static int intHash(int key) {
//		int w = 4; // Number of bits
//		int p = Global.partitions; // number of slots i.e., 16 partitions
//		int m = 2^p; // 
//		int s = 13; // Must have 0 < s < 2^w. Let, s = 9973 (Prime number)
//		int A  = s/2^w; // Or, 0.5*(sqrt(5) - 1)
////		
////		// h(k) = ⌊m · (k·A mod 1)⌋
//		return (int) Math.floor(m*((key*A)%1));
		
		
		//--------------------------------------------
//		int s = (int) Math.floor((double)(key * 2^w));
//		int x = k*s;
//		return (x >> (w-p));
		
		//--------------------------------------------
//		int p = 20; // m = 2^20
//	    int w = 32;
//	    int A = (int) 2654435769L;
//	    
//		return (key * A) >>> (w - p);
		
		//--------------------------------------------
		double A = 0.6180339887;
		int m = 65536; //2^(Global.partitions);
		
		return (int) Math.floor(m * ((key * A) % 1));
	}
	
	public static String getRandomAlphanumericString() {
		return RandomStringGenerator.generateRandomString(50, RandomStringGenerator.Mode.ALPHANUMERIC);
	}
	
	// Returns a SHA1 hash value
	public static long sha512Hash(String key) {		
		String value = DigestUtils.sha512Hex(key.getBytes());		
		BigInteger b = new BigInteger(value, 16);		
		
		return (b.longValue());
	}
	
/*	// Returns a SHA1 hash value
	public static long sha1Hash(String key, boolean lookup, boolean flag) {
		
		if(flag) {
			if(lookup) {
				int prefix = Global.global_index_map.get(Integer.parseInt(key));
				key = (Integer.toString(prefix) + key);
			} else {
				key = getSaltedKey(key);
			}			
			
			BigInteger bigInt = new BigInteger(key.getBytes());
			key = bigInt.toString();
			key = getAlphaNumericString(key);
			//System.out.println(key);
		}
		
		byte[] value = DigestUtils.sha1(key.getBytes());
		return Utility.convertByteToUnsignedLong(value);
	}
*/
	
	public static String getAlphaNumericString(String str) {
		String out = null;
		
		try {
		    byte[] b = str.getBytes("ASCII");
		    MessageDigest md = MessageDigest.getInstance("SHA-256");
		    
		    byte[] hashBytes = md.digest(b);
		    StringBuffer hexString = new StringBuffer();
		    
		    for (int i = 0; i < hashBytes.length; i++)
		        hexString.append(Integer.toHexString(0xFF & hashBytes[i]));
		    
		    out = hexString.toString();
		    
		} catch (Exception e) {
		    e.printStackTrace();
		}
		
		return out;
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
	
	// 
	public static PrintWriter getPrintWriter(String dir, File file) {		
		PrintWriter prWriter = null;
		
		try {
			file.getParentFile().mkdirs();
			
			/*if(file.exists())
				file.delete();*/
			
			file.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));	
				
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
	 
	public static boolean isOSX() {
		return (Global.OS.indexOf("mac") >= 0);
	}	
	
	/**
	 * Added from SO: http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
	 * 
	 */
	public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
		
	    Comparator<K> valueComparator =  new Comparator<K>() {
	        public int compare(K k1, K k2) {
	            int compare = map.get(k2).compareTo(map.get(k1));
	            
	            if (compare == 0) 
	            	return 1;
	            else 
	            	return compare;
	        }
	    };
	    
	    Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
	    sortedByValues.putAll(map);
	    
	    return sortedByValues;
	}
	
	public static <K,V extends Comparable<? super V>> List<Entry<K, V>> sortedByValuesAsc(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());

		Collections.sort(sortedEntries, new Comparator<Entry<K,V>>() 
			{
				@Override
				public int compare(Entry<K,V> e1, Entry<K,V> e2) {
					return e1.getValue().compareTo(e2.getValue()); // Ascending Order
				}
			}
		);

		return sortedEntries;
	}
}	