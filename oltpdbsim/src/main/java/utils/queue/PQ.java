package main.java.utils.queue;

import java.util.Comparator;
import java.util.PriorityQueue;
import main.java.utils.graph.CompressedHEdge;

@SuppressWarnings("serial")
public class PQ<E> extends PriorityQueue<E> {

	public PQ(int i, Comparator<CompressedHEdge> reverseOrder) {
		super();
	}

	//Will not contain/allow any duplicates
	@Override
	public boolean add(E e) {
		boolean isAdded = false;
     
		if(!super.contains(e))
			isAdded = super.add(e);
   
		return isAdded;
	}
}