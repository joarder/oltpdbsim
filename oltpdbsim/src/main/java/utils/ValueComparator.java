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