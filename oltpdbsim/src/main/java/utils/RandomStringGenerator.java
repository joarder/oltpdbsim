package main.java.utils;

import main.java.entry.Global;

public class RandomStringGenerator {
	
	public static enum Mode {
	    ALPHA, ALPHANUMERIC, NUMERIC 
	}
	
	public static String generateRandomString(int length, Mode mode) {

		StringBuffer buffer = new StringBuffer();
		String characters = "";

		switch(mode) {		
			case ALPHA:
				characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
				break;
		
			case ALPHANUMERIC:
				characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
				break;
	
			case NUMERIC:
				characters = "1234567890";
				break;
		}
		
		int charactersLength = characters.length();
		
		for (int i = 0; i < length; i++) {
			double index = Global.rand.nextDouble() * charactersLength;			
			buffer.append(characters.charAt((int) index));
		}
		
		return buffer.toString();
	}
}