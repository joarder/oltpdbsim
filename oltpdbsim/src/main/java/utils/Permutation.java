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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

public class Permutation {
	
	public static Set<IntPair> getPermutations(Set<Integer> list) {
		Set<IntPair> permutations = new HashSet<IntPair>();
		
		int[] array = ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()]));

		for(int i = 0; i < array.length; i++)
		   for(int j = i+1; j < array.length; j++)
			   permutations.add(new IntPair(array[i], array[j]));
		
		return permutations;		
	}
}