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

import java.util.Comparator;
import java.util.Map;

public class ValueComparator<T extends Comparable<T>> implements Comparator<T> {

    Map<T, T> map;

    public ValueComparator(Map<T, T> map) {
        this.map = map;
    }

    @Override
    public int compare(T o1, T o2) {
    	return map.get(o2).compareTo(map.get(o1));
    }
}