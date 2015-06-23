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

package main.java.utils.queue;

import java.util.LinkedList;

import umontreal.iro.lecuyer.randvar.ExponentialGen;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.MRG32k3a;
import umontreal.iro.lecuyer.simevents.Accumulate;
import umontreal.iro.lecuyer.simevents.Event;
import umontreal.iro.lecuyer.simevents.Sim;
import umontreal.iro.lecuyer.stat.Tally;

public class QueueEv {

	RandomVariateGen genArr;
	RandomVariateGen genServ;
	
	LinkedList<Customer> waitList = new LinkedList<Customer> ();
	LinkedList<Customer> servList = new LinkedList<Customer> ();
	
	Tally custWaits = new Tally ("Waiting times");
	Accumulate totWait = new Accumulate ("Size of queue");
	
	class Customer { double arrivTime, servTime; }
	
	public QueueEv (double lambda, double mu) {
		genArr = new ExponentialGen (new MRG32k3a(), lambda);
		genServ = new ExponentialGen (new MRG32k3a(), mu);
	}
	
	public void simulateOneRun (double timeHorizon) {
		Sim.init();
		new EndOfSim().schedule (timeHorizon);
		new Arrival().schedule (genArr.nextDouble());
		Sim.start();
	}
	
	class Arrival extends Event {
		public void actions() {
			new Arrival().schedule (genArr.nextDouble()); // Next arrival.
		
			Customer cust = new Customer(); // Cust just arrived.
			cust.arrivTime = Sim.time();
			cust.servTime = genServ.nextDouble();
		
			if (servList.size() > 0) {
				// Must join the queue.
				waitList.addLast (cust);
				totWait.update (waitList.size());
			} else {
				// Starts service.
				custWaits.add (0.0);
				servList.addLast (cust);
				new Departure().schedule (cust.servTime);
			}
		}
	}

	class Departure extends Event {
		public void actions() {
			servList.removeFirst();

			if (waitList.size() > 0) {
				// Starts service for next one in queue.
				Customer cust = waitList.removeFirst();
				totWait.update (waitList.size());
				custWaits.add (Sim.time() - cust.arrivTime);
				servList.addLast (cust);
				new Departure().schedule (cust.servTime);
			}
		}
	}
	
	class EndOfSim extends Event {
		public void actions() {
			Sim.stop();
		}
	}

	public static void main (String[] args) {
		QueueEv queue = new QueueEv (1.0, 2.0);
		queue.simulateOneRun (1000.0);
		
		System.out.println (queue.custWaits.report());
		System.out.println (queue.totWait.report());
	}
}