package main.java.repartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.entry.Global;
import main.java.utils.graph.SimpleHEdge;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class Analysis {	
	
	public static void analyse(WorkloadBatch wb) {
		performAnalysis(wb, Global.servers);
		
		System.out.println();
		System.out.println("Analysis having a faulty server in the system ...");		
		performAnalysis(wb, Global.servers - 1);
	}
	
	static Map<Integer, List<T_dummy>> tMap;
	
	public static void performAnalysis(WorkloadBatch wb, int servers) {
		
		tMap = new HashMap<Integer, List<T_dummy>>();
		
		int groups = 0;		
		int transactions = wb.hgr.getEdgeCount();
		int denial_of_service = 0;
		int highest_span = Integer.MIN_VALUE;
		int lowest_span = Integer.MAX_VALUE;
		
		System.out.println("HGraph Size = "+wb.hgr.getEdgeCount());
		
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());
			System.out.println(tr.toString());
			int t_span = tr.getTr_serverSet().size();
			T_dummy t = new T_dummy(tr.getTr_id(), t_span, tr.getTr_period());
			
			if(tMap.containsKey(t_span)) {
				tMap.get(t_span).add(t);
				
			} else {
				List<T_dummy> tList = new ArrayList<T_dummy>();
				tList.add(t);
				tMap.put(t_span, tList);
				
				if(t_span < lowest_span)
					lowest_span = t_span;
				else if(t_span > highest_span)
					highest_span = t_span;
			}
		}
		
		System.out.println("Size = "+tMap.size());
		System.out.println("HSpan = "+highest_span);
		System.out.println("LSpan = "+lowest_span);
		
		if(highest_span > servers) {
			denial_of_service = tMap.get(highest_span).size();
			transactions -= denial_of_service;
			
			tMap.remove(highest_span);
			--highest_span;
		}
		
		// Scheduling	
		System.out.println("Size = "+tMap.size());
		while(tMap.size() != 0) {			
			if(tMap.containsKey(highest_span)) {				
					++groups;
					//System.out.println("--> Group-"+groups);
					
					T_dummy t = tMap.get(highest_span).remove(0);
					//System.out.println(t.toString());			
	
					// Adjust
					if(tMap.get(highest_span).size() == 0) {
						tMap.remove(highest_span);
						--highest_span;
					}				
					
					if(t.span != servers) {
						int req = servers - t.span;
	
						if(tMap.containsKey(req)) {						
							T_dummy t1 = tMap.get(req).remove(0);
							//System.out.println(t1.toString());
	
							// Adjust
							if(tMap.get(req).size() == 0) {
								tMap.remove(req);
								
								if(lowest_span == req)
									++lowest_span;
							}
							
						} else {
							if(req > lowest_span) {
								for(int i = 0; i < req; ++i) {
									T_dummy t2 = tMap.get(lowest_span).remove(0);
									//System.out.println(t2.toString());
							
									// Adjust
									if(tMap.get(lowest_span).size() == 0) {
										tMap.remove(lowest_span);
										++lowest_span;
									} //end-if
								} //end-for
							} //end-if
						} //end-else-if
					} //end-if
				} //end-if
		} //end-while()
		
		double parallelism = (double) (transactions + denial_of_service)/(groups + denial_of_service);
		
		System.out.println("-------------------------------------------------------------------");
		System.out.println("Total transactions awaiting service = "+wb.hgr.getEdgeCount());
		System.out.println("Total transactions processed = "+transactions);
		System.out.println("Total transactions denied = "+denial_of_service);
		System.out.println("Total groups = "+(groups + denial_of_service));
		System.out.println("Total groups for denied transactions = "+denial_of_service);
		System.out.println("Parallelism achieved = "+parallelism);
	}
}

class T_dummy {
	int id;
	int span;
	double period;
	
	T_dummy(int id, int span, double period) {
		this.id = id;
		this.span = span;
		this.period = period;
	}
	
	@Override
	public String toString() {
		return ("\t>> T"+this.id+"("+this.span+")");
	}
}