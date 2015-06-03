package main.java.dsm;

import java.util.HashSet;

public class FCICluster{
	public double weight;		
	public HashSet<Integer> fci;
	
	public FCICluster() {
		this.weight = 1;
		this.fci = new HashSet<Integer>();
	}
	
	@Override
	public String toString() {
		return ("["+this.weight+"{"+this.fci+"}]");
	}
}