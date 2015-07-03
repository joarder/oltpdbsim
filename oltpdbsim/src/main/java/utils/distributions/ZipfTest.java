package main.java.utils.distributions;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfTest {

	public static void main(String[] args) {
		ZipfDistribution z1 = new ZipfDistribution(10, 1.75);
		ZipfianGenerator z2 = new ZipfianGenerator(10, 1.75);
		ScrambledZipfianGenerator z3 = new ScrambledZipfianGenerator(10);
		
		for(int i = 0; i < 20; i++)
			System.out.println("--> z1="+z1.sample()+" | z2="+(z2.nextInt()+1)+" | z3="+(z3.nextInt()+1));
		
		z1.reseedRandomGenerator(1);
		
		for(int i = 0; i < 20; i++)
			System.out.println(">> z1="+z1.sample()+" | z2="+(z2.nextInt()+1)+" | z3="+(z3.nextInt()+1));
	}
}