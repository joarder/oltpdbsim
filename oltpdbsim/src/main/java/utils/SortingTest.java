package main.java.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;

public class SortingTest {

	static double income_preference = 0.5;
	static double age_preference = 1 - income_preference;
	
	@SuppressWarnings("rawtypes")
	static TreeSet ageRank = new TreeSet();
	@SuppressWarnings("rawtypes")
	static TreeSet incomeRank = new TreeSet();
	
	@SuppressWarnings("unchecked")
	public static void main(String args[]) {
		ArrayList<Person> persons = new ArrayList<Person>();
		
		persons.add(new Person("A", 60, 55.0));
		persons.add(new Person("B", 45, 50.0));
		persons.add(new Person("C", 20, 50.0));
		persons.add(new Person("D", 55, 60.0));
		persons.add(new Person("E", 30, 85.0));
		
		/*// Sort the array list by income (descending order)
		Collections.sort(persons, new Comparator<Person>(){
			@Override
			public int compare(Person p1, Person p2) {				
				return (((double)p1.income > (double)p2.income) ? -1 : 
					((double)p1.income < (double)p2.income) ? 1 : 0);				
			}
		});
		
		// Rank based on income
		int income_rank = persons.size();
		for(int i = 0; i < persons.size(); i++) {
			if(i != 0)
				if(persons.get(i).income != persons.get(i-1).income)
					--income_rank;
			
			persons.get(i).income_rank = income_rank * income_preferrence;
		}
		
		System.out.println("List of persons sorted by their income in descending order: ");
		for(Person p : persons) 
			System.out.println(p.toString());		
		
		// Sort the array list by age (ascending order)
		Collections.sort(persons, new Comparator<Person>(){
			@Override
			public int compare(Person p1, Person p2) {				
				return (((double)p2.age > (double)p1.age) ? -1 : 
					((double)p2.age < (double)p1.age) ? 1 : 0);				
			}
		});
		
		// Rank based on age
		int age_rank = persons.size();
		for(int i = 0; i < persons.size(); i++) {
			if(i != 0)
				if(persons.get(i).age != persons.get(i-1).age)
					--age_rank;
			
			persons.get(i).age_rank = age_rank * age_preferrence;
		}		
		
		System.out.println();
		System.out.println("List of persons sorted by their age in ascending order: ");
		for(Person p : persons) 
			System.out.println(p.toString());*/		
		
		for(Person p : persons) {
			   ageRank.add(p.age);
			   incomeRank.add(p.income);
		}		
		
		System.out.println("ageRank: "+ageRank);
		System.out.println("incomeRank: "+incomeRank);
		
		// Assign combined rank		
		/*for(Person p : persons)
			p.combined_rank = (p.age_rank + p.income_rank);*/
		
		// Sort the array list by the value of combined rank (descending order)
		Collections.sort(persons, new Comparator<Person>(){
			@SuppressWarnings("rawtypes")
			@Override
			public int compare(Person p1, Person p2) {
								
				int ageRank1 = ((TreeSet) ageRank).tailSet(p1.age).size();
	            int ageRank2 = ((TreeSet) ageRank).tailSet(p2.age).size();
	            int incomeRank1 = ((TreeSet) incomeRank).headSet(p1.income).size();
	            int incomeRank2 = ((TreeSet) incomeRank).headSet(p2.income).size();				
				
	            System.out.println("----------------");
	            p1.combined_rank = ageRank1*age_preference + incomeRank1*income_preference;
	            System.out.println(p1.toString());
	            System.out.println("P1: combined_rank = "+p1.combined_rank+" | ageRank = "+ageRank1+" | incomeRank = "+incomeRank1);
	            
	            p2.combined_rank = ageRank2*age_preference + incomeRank2*income_preference;
	            System.out.println(p2.toString());
	            System.out.println("P2: combined_rank = "+p2.combined_rank+" | ageRank = "+ageRank2+" | incomeRank = "+incomeRank2);
	            
				return (((double)p1.combined_rank > (double)p2.combined_rank) ? -1 : 
					((double)p1.combined_rank < (double)p2.combined_rank) ? 1 : 0);				
			}
		});
		
		System.out.println();
		System.out.println("List of persons sorted by their ranking preferrence in descending order: ");
		for(Person p : persons) 
			System.out.println(p.toString());
	}
}

class Person {	
	String name;
	int age; // lower is better
	double income; // higher is better
	double age_rank;
	double income_rank;
	double combined_rank;
	
	public Person(String name, int age, double income) {
		this.name = name;
		this.age = age;
		this.income = income;
		this.age_rank = 0.0;
		this.income_rank = 0.0;
		this.combined_rank = 0.0;
	}
	
	@Override
	public String toString() {
		return ("Person-"+this.name+", age("+this.age+"|"+this.age_rank+"th), income("+this.income+"|"+this.income_rank+"th), Combined Rank("+this.combined_rank+")");
	}
}