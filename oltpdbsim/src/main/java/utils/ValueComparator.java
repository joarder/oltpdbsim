package main.java.utils;

import java.util.Comparator;
import java.util.Map;

//public class ValueComparator implements Comparator<Object> {
//
//    Map<Integer, Integer> map;
//
//    public ValueComparator(Map<Integer, Integer> map) {
//        this.map = map;
//    }
//
//    public int compare(Object o1, Object o2) {
//
//        if (map.get(o2) == map.get(o1))
//            return 1;
//        else
//            return ((Integer) map.get(o2)).compareTo((Integer) map.get(o1));
//    }
//}


public class ValueComparator<T extends Comparable<T>> implements Comparator<T> {

    Map<T, T> map;

    public ValueComparator(Map<T, T> map) {
        this.map = map;
    }

    @Override
    public int compare(T o1, T o2) {
    	return map.get(o1).compareTo(map.get(o2));
    }
}