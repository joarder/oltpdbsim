/*
 *    Utilities.java
 *    Copyright (C) 2012 Universitat Politècnica de Catalunya
 *    @author Massimo Quadrana <max.square@gmail.com>
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package main.java.incmine.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
//import moa.learners.IncMine;
//import moa.streams.ZakiFileStream;
import main.java.incmine.learners.IncMine;
import main.java.incmine.streams.ZakiFileStream;

public class Utilities {
    
    private Utilities(){};
    
    /**
     * Computes the intersection of 2 integer ordered lists.
     * @param s1 shorter set 
     * @param s2 longer set
     * @return intersection result
     */
    public static List<Integer> intersect2orderedList(List<Integer> s1, List<Integer> s2) {
        
        if(s1.size() > s2.size())
            return intersect2orderedList(s2,s1);
        
        List<Integer> res = new ArrayList<Integer>();
        int pos1 = 0, pos2 = 0;

        while(pos1 < s1.size() && pos2 < s2.size()) {
            
            if(s1.get(pos1).equals(s2.get(pos2))){
                res.add(s1.get(pos1));
                pos1++;
                pos2++;
            }else{
                if(s1.get(pos1) < s2.get(pos2))
                    pos1++;
                else
                    pos2++;
            }
        }

        return res;
    }
    
    
    /**
     * Computes the cumulative sum of the first k elements of the passed vector
     * @param vector integer vector
     * @param k number of elements
     * @return sum of the first k elements in vector
     */
    public static int cumSum(int[] vector, int k) {
        int sum = 0;
        
        k = k > vector.length-1 ? vector.length -1 : k;
        
        for(int i = 0; i <= k && i < vector.length ; i++)
            sum += vector[i];

        return sum;
    }
    
    public static int[] getIncMineMinSupportVector(double sigma, double r, int windowSize,int nSegments){
        
        int[] supVector = new int[windowSize];

        for(int k=0; k<windowSize; k++)
            supVector[k] =(int) Math.ceil(nSegments * sigma * ((1 - r) / windowSize * k + r));

        return supVector;
    }
    
    public static double mean(List<Double> values){
        double sum = 0;
        for(Double value:values)
            sum += value;
        
        return sum / (double) values.size();
    }
    
    public static List<FrequentItemset> last_fi_set;
    
    public static void runOnStreamDrift(ZakiFileStream stream, IncMine learner, BufferedWriter outRes,
            BufferedWriter outFi, Map<Integer,String> dict,int startInstance, int stopInstance) throws IOException{
        stream.restart();
        
        int i = 0;
        int k = 0;
        List<Integer> fi_size_list = new ArrayList<Integer>();
        List<Double> throughput_list = new ArrayList<Double>();
        
        List<Integer> fci_list = new ArrayList<Integer>();
        List<Integer> fci_add_list = new ArrayList<Integer>();
        List<Integer> fci_rem_list = new ArrayList<Integer>();
        
        List<Integer> unfrequent_list = new ArrayList<Integer>();
        List<Integer> still_frequent_list = new ArrayList<Integer>();
        List<Integer> new_frequent_list = new ArrayList<Integer>();
        
        last_fi_set = null;
        
        while(stream.hasMoreInstances() && (stopInstance == -1 || i < stopInstance)){
            if(i < startInstance){
                stream.nextInstance();
                i++;
                continue;
            }
            learner.trainOnInstance(stream.nextInstance());
            if(++i % learner.fixedSegmentLengthOption.getValue() == 0){ //wait for sliding 
                System.out.println("TRANSACTION N." + i);
                //get the current set of frequent itemset
                k = k == learner.windowSizeOption.getValue() ? k : k+1;
                
                List<FrequentItemset> fi_set = FrequentItemset.getFIset(learner.getApproximateFCIIterator(),
                        learner.minSupportOption.getValue(), learner.fixedSegmentLengthOption.getValue());
                
                if(last_fi_set == null){
                    last_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    unfrequent_list.add(0);
                    still_frequent_list.add(0);
                    new_frequent_list.add(0);
                }
                else{
                    //compare the actual FI set with the passed one to find drift
                    int still_fi_counter = 0;
                    
                    List<FrequentItemset> new_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    
                    for(FrequentItemset fi : last_fi_set)
                        if(new_fi_set.remove(fi))
                            still_fi_counter++;
                    
                    int nomore_fi_counter = last_fi_set.size() - still_fi_counter;
                    int new_fi_counter = new_fi_set.size();
                    
                    
                    System.out.println(nomore_fi_counter + " itemsets become unfrequent");
                    System.out.println(still_fi_counter + " itemsets are still frequent");
                    System.out.println(new_fi_counter + " itemsets become frequent");
                    
                    unfrequent_list.add(nomore_fi_counter);
                    still_frequent_list.add(still_fi_counter);
                    new_frequent_list.add(new_fi_counter);
                    
                    last_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    
                    double unfrequent_rate = (double) nomore_fi_counter / (double) last_fi_set.size();
                    double new_frequent_rate = (double) new_fi_counter / (double) last_fi_set.size();
                    
                    if(unfrequent_rate > 0.2 || new_frequent_rate > 0.2){ 
                        //Drift is occuring, store actual fi set
                        outFi.write("TRANSACTION N." + i + "\n");
                        Collections.sort(fi_set, new Comparator<FrequentItemset>(){

                            public int compare(FrequentItemset o1, FrequentItemset o2) {
                                return o2.support - o1.support;
                            }
                        });
                        int n_out = 0;
                        for(FrequentItemset fi:fi_set){
                            if(fi.size == 1)
                                continue;
                             if(n_out > 10)
                                break;
                            outFi.write("[");
                            for(Integer item : fi.items)
                                outFi.write(dict.get(item) + "; ");
                            outFi.write("]: "+ fi.support + "\n");
                            n_out++;
                           
                        }
                    }
                }
                
                double tp = (double) learner.fixedSegmentLengthOption.getValue() / (double) learner.getUpdateTime() * 1e9;
                
                fci_list.add(learner.getNumFCIs());
                fci_add_list.add(learner.getNAdded());
                fci_rem_list.add(learner.getNRemoved());
                
                fi_size_list.add(fi_set.size());
                throughput_list.add(tp);
                System.out.println("Total FIs: " + fi_set.size() + "\n");
            }
        }
        
        outRes.write("fci = " + fci_list + ";\n");
        outRes.write("fci_add = " + fci_add_list + ";\n");
        outRes.write("fci_rem = " + fci_rem_list + ";\n");
        outRes.write("unfreq = " + unfrequent_list + ";\n");
        outRes.write("still_freq = " + still_frequent_list + ";\n");
        outRes.write("newfreq = " + new_frequent_list + ";\n");       
        outRes.write("fi = " + fi_size_list + ";\n");        
        outRes.write("tp = " + throughput_list + ";\n");
    }
    
    public static void runOnStreamDrift(ZakiFileStream stream, IncMine learner, BufferedWriter out, int startInstance, int stopInstance) throws IOException{
        stream.restart();
        
        int i = 0;
        int k = 0;
        List<Integer> fi_size_list = new ArrayList<Integer>();
        List<Double> throughput_list = new ArrayList<Double>();
        
        List<Integer> fci_list = new ArrayList<Integer>();
        List<Integer> fci_add_list = new ArrayList<Integer>();
        List<Integer> fci_rem_list = new ArrayList<Integer>();
        
        List<Integer> unfrequent_list = new ArrayList<Integer>();
        List<Integer> still_frequent_list = new ArrayList<Integer>();
        List<Integer> new_frequent_list = new ArrayList<Integer>();
        
        last_fi_set = null;
        
        while(stream.hasMoreInstances() && (stopInstance == -1 || i < stopInstance)){
            if(i < startInstance){
                stream.nextInstance();
                i++;
                continue;
            }
            learner.trainOnInstance(stream.nextInstance());
            if(++i % learner.fixedSegmentLengthOption.getValue() == 0){ //wait for sliding 
                System.out.println("TRANSACTION N." + i);
                //get the current set of frequent itemset
                k = k == learner.windowSizeOption.getValue() ? k : k+1;
                
                List<FrequentItemset> fi_set = FrequentItemset.getFIset(learner.getApproximateFCIIterator(),
                        learner.minSupportOption.getValue(), learner.fixedSegmentLengthOption.getValue());
                
                if(last_fi_set == null){
                    last_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    unfrequent_list.add(0);
                    still_frequent_list.add(0);
                    new_frequent_list.add(0);
                }
                else{
                    //compare the actual FI set with the passed one to find drift
                    int still_fi_counter = 0;
                    
                    List<FrequentItemset> new_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    
                    for(FrequentItemset fi : last_fi_set)
                        if(new_fi_set.remove(fi))
                            still_fi_counter++;
                    
                    int nomore_fi_counter = last_fi_set.size() - still_fi_counter;
                    int new_fi_counter = new_fi_set.size();
                    
                    
                    System.out.println(nomore_fi_counter + " itemsets become unfrequent");
                    System.out.println(still_fi_counter + " itemsets are still frequent");
                    System.out.println(new_fi_counter + " itemsets become frequent");
                    
                    unfrequent_list.add(nomore_fi_counter);
                    still_frequent_list.add(still_fi_counter);
                    new_frequent_list.add(new_fi_counter);
                    
                    last_fi_set = new ArrayList<FrequentItemset>(fi_set);
                    
//                    double unfrequent_rate = (double) nomore_fi_counter / (double) last_fi_set.size();
//                    double new_frequent_rate = (double) new_fi_counter / (double) last_fi_set.size();
                    
                }
                
                double tp = (double) learner.fixedSegmentLengthOption.getValue() / (double) learner.getUpdateTime() * 1e9;
                
                fci_list.add(learner.getNumFCIs());
                fci_add_list.add(learner.getNAdded());
                fci_rem_list.add(learner.getNRemoved());
                
                fi_size_list.add(fi_set.size());
                throughput_list.add(tp);
                System.out.println("Total FIs: " + fi_set.size() + "\n");
            }
        }
        
        int w = learner.windowSizeOption.getValue();
        
        out.write("fci_" + w + " = " + fci_list + ";\n");
        out.write("fci_add_" + w + " = " + fci_add_list + ";\n");
        out.write("fci_rem_" + w + " = " + fci_rem_list + ";\n");
        out.write("unfreq_" + w + " = " + unfrequent_list + ";\n");
        out.write("still_freq_" + w + " = " + still_frequent_list + ";\n");
        out.write("newfreq_" + w + " = " + new_frequent_list + ";\n");       
        out.write("fi_" + w + " = " + fi_size_list + ";\n");        
        out.write("tp_" + w + " = " + throughput_list + ";\n");
    }
    
    static public final long MEGABYTE_SIZE = 1024L * 1024L;
    public static double getMemoryUsage(){
        
        
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return (double)usedMemory / (double)MEGABYTE_SIZE;
    }
}
