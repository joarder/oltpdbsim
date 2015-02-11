package main.java.utils;

import java.util.Random;

import main.java.entry.Global;

//import org.apache.commons.lang3.RandomStringUtils;

public class Test {
	public static void main(String[] args) {
		
		Global.rand = new Random();
		Global.rand.setSeed(0);		
		
		for(int i = 0; i < 5; i++) {
		    //System.out.println(RandomStringUtils.randomAlphanumeric(10));
			System.out.println(RandomStringGenerator.generateRandomString(10, RandomStringGenerator.Mode.ALPHANUMERIC));
		}
	}
}