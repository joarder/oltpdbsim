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
 
/**
 * A simulation of Lindley’s recurrence using observers
 */

package main.java.mm1;

import java.util.*;
import umontreal.iro.lecuyer.stat.*;
import umontreal.iro.lecuyer.rng.*;
import umontreal.iro.lecuyer.randvar.*;

public class QueueObs {
	Tally waitingTimes = new Tally ("Waiting times");
	Tally averageWaits = new Tally ("Average wait");

	RandomVariateGen genArr;
	RandomVariateGen genServ;

	int cust; // Number of the current customer.

	public QueueObs (double lambda, double mu, int step) {
		genArr = new ExponentialGen (new MRG32k3a(), lambda);
		genServ = new ExponentialGen (new MRG32k3a(), mu);
		waitingTimes.setBroadcasting (true);
		waitingTimes.addObservationListener (new ObservationTrace (step));
		waitingTimes.addObservationListener (new LargeWaitsCollector (2.0));
	}

	public double simulateOneRun (int numCust) {
		waitingTimes.init();
		double Wi = 0.0;
		waitingTimes.add (Wi);

		for (cust = 2; cust <= numCust; cust++) {
			Wi += genServ.nextDouble() - genArr.nextDouble();

			if (Wi < 0.0) 
				Wi = 0.0;
			
			waitingTimes.add (Wi);
		}

		return waitingTimes.average();
	}

	public void simulateRuns (int n, int numCust) {
		averageWaits.init();

		for (int i=0; i<n; i++)
			averageWaits.add (simulateOneRun (numCust));
	}

	public class ObservationTrace implements ObservationListener {
		private int step;
		
		public ObservationTrace (int step) { this.step = step; }
		
		public void newObservation (StatProbe probe, double x) {
			if (cust % step == 0)
				System.out.println ("Customer " + cust + " waited "+ x + " time units.");
		}
	}

	public class LargeWaitsCollector implements ObservationListener {
		double threshold;
		ArrayList<Double> largeWaits = new ArrayList<Double>();

		public LargeWaitsCollector (double threshold) {
			this.threshold = threshold;
		}

		public void newObservation (StatProbe probe, double x) {
			if (x > threshold) 
				largeWaits.add (x);
		}

		public String formatLargeWaits () {
			// Should print the list largeWaits.
			return "not yet implemented...";
		}
	}

	public static void main (String[] args) {
		QueueObs queue = new QueueObs (1.0, 2.0, 5);
		queue.simulateRuns (2, 100);

		System.out.println ("\n\n" + queue.averageWaits.report());
	}
}