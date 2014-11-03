package main.java.utils;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

public class Permutation {
	
	public static Set<VertexPair> getPermutations(Set<Integer> list) {
		Set<VertexPair> permutations = new HashSet<VertexPair>();
		
		int[] array = ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()]));

		for(int i = 0; i < array.length; i++)
		   for(int j = i+1; j < array.length; j++)
			   permutations.add(new VertexPair(array[i], array[j]));
		
		return permutations;		
	}
}