package main.java.repartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.entry.Global;
import main.java.metric.PerfMetric;
import main.java.utils.graph.SimpleHEdge;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class Analysis {	
	
	public static void analyse(WorkloadBatch wb, PerfMetric perfm) {
		++Global.repartitioningCycle;
		
		for(int i = 0; i < Global.servers/2+1; i++) {
			System.out.println("-------------------------------------------------------");
			System.out.println("Analysis having "+i+" faulty servers in the cluster ...");
			
			performAnalysis(wb, Global.servers - i, perfm);
		}
	}
	
	static Map<Integer, List<T_dummy>> tMap;
	
	public static double performAnalysis(WorkloadBatch wb, int servers, PerfMetric perfm) {
		
		tMap = new HashMap<Integer, List<T_dummy>>();
		
		int groups = 0;		
		int transactions = wb.hgr.getEdgeCount();
		//int processed = 0;
		int denial_of_service = 0;
		int highest_span = Integer.MIN_VALUE;
		int lowest_span = Integer.MAX_VALUE;
		
		//System.out.println("HGraph Size = "+wb.hgr.getEdgeCount());
		
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());			
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
				
				if(t_span > highest_span)
					highest_span = t_span;
			}
			
			//System.out.println("t_span = "+t_span+" | highest_span = "+highest_span+" | lowest_span = "+lowest_span);
		}
		
		perfm.prWriter3.print(Global.repartitioningCycle+" "+Global.servers+" "+servers+" "
				+highest_span+" "+lowest_span+" ");
		
		//System.out.println("Size = "+tMap.size());
		//System.out.println("highest_span = "+highest_span);
		//System.out.println("lowest_span = "+lowest_span);
		
		if(highest_span > servers) {
			denial_of_service = tMap.get(highest_span).size();
			transactions -= denial_of_service;
			
			tMap.remove(highest_span);
			--highest_span;
		}
		
		// Scheduling	
		//System.out.println("Scheduling transactions having "+tMap.size()+" different spans within "+servers+" physical machines ...");
		//System.out.println("[# of Transactions X Span]");
		/*for(Entry<Integer, List<T_dummy>> entry : tMap.entrySet()) {
			System.out.println(+entry.getValue().size()+" X "+entry.getKey());
		}*/		
		
		while(tMap.size() != 0) {	
			//int total_span_in_group = 0;
			
			if(tMap.containsKey(highest_span)) {				
				++groups;
				
				/*System.out.println("---------------------------------------------------------------");
				System.out.println("--> Group # "+groups+" | Servers = "+servers+" | Transactions Processed = "+processed);
				System.out.println("---------------------------------------------------------------");
				
				System.out.println("[# of Transactions X Span]");
				for(Entry<Integer, List<T_dummy>> entry : tMap.entrySet()) {
					System.out.println(+entry.getValue().size()+" X "+entry.getKey());
				}*/
				
				//System.out.println("--> highest_span = "+highest_span+" | lowest_span = "+lowest_span);
				
				T_dummy t = null;
				if(!tMap.get(highest_span).isEmpty()) {
					t = tMap.get(highest_span).remove(0);
					//++processed;
					//total_span_in_group += highest_span;
					//System.out.println(t.toString());
				}

				// Adjust
				if(tMap.get(highest_span).size() == 0) {
					tMap.remove(highest_span);
					--highest_span;
					
					while(!tMap.containsKey(highest_span) && tMap.size() != 0)
						--highest_span;
				}				
				
				if(t.span != servers && t != null) {
					int required = servers - t.span;
					//System.out.println("\tInitially required = "+required);

					if(tMap.containsKey(required)) {
						
						if(!tMap.get(required).isEmpty()) {
							tMap.get(required).remove(0); //T_dummy t1 = 
							//++processed;
							//total_span_in_group += required;
							//System.out.println(t1.toString());
						}

						// Adjust
						if(tMap.get(required).size() == 0) {
							tMap.remove(required);
							
							if(lowest_span == required) {
								++lowest_span;
								
								while(!tMap.containsKey(lowest_span) && tMap.size() != 0)
									++lowest_span;
								
							} else if(highest_span == required) {
								--highest_span;
								
								while(!tMap.containsKey(highest_span) && tMap.size() != 0)
									--highest_span;
							}								
						}
						
					} else {
						
						if(required > lowest_span) {
							
							while(required > 0 && required >= lowest_span && !tMap.isEmpty()) {
								
								int difference = required - lowest_span;
								//System.out.println("\tDifference = "+difference+" | Required = "+required);																
								
								if(!tMap.containsKey(difference)) {
									
									if(tMap.containsKey(highest_span) && required >= highest_span) {
										//System.out.println("\tChoosing highest span transaction ...");
										difference = highest_span;
										
									} else {										
										//System.out.println("\tChoosing lowest span transaction ...");
										difference = lowest_span;
									}
																		
									//System.out.println("\tAdjusted difference = "+difference);
								}
						
								if(!tMap.get(difference).isEmpty()) {
									tMap.get(difference).remove(0); //T_dummy t2 = 
									//++processed;
									//total_span_in_group += difference;
									//System.out.println(t2.toString());
								}
								
								// Adjust
								if(tMap.get(difference).size() == 0) {
									tMap.remove(difference);
									
									if(lowest_span == difference) {
										++lowest_span;
										
										while(!tMap.containsKey(lowest_span) && tMap.size() != 0)
											++lowest_span;
										
									} else if(highest_span == difference) {
										--highest_span;
										
										while(!tMap.containsKey(highest_span) && tMap.size() != 0)
											--highest_span;
									}
								} //end-if
								
								required -= difference;
								
							} //end-whilte()
						} //end-if
					} //end-else-if
				} //end-if
				
				//System.out.println("Total transactional span in Group #" +groups+" = "+total_span_in_group);
				//System.out.println("->> highest_span = "+highest_span+" | lowest_span = "+lowest_span);
			} //end-if
		} //end-while()
		
		double parallelism = (double) (transactions + denial_of_service)/(groups + denial_of_service);
				
		System.out.println("Total transactions awaiting service = "+wb.hgr.getEdgeCount());
		System.out.println("Total transactions processed = "+transactions);
		System.out.println("Total transactions denied = "+denial_of_service);
		System.out.println("Total groups = "+(groups + denial_of_service));
		System.out.println("Parallelism achieved = "+parallelism);
		
		// Write		
		perfm.prWriter3.print(wb.hgr.getEdgeCount()+" "+transactions+" "+denial_of_service+" "
									+(groups+denial_of_service+" "+parallelism));		
		perfm.prWriter3.println();
			
		
		return parallelism;
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