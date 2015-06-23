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
 * A simulation based on Lindley’s recurrence
 */

package main.java.mm1;

import umontreal.iro.lecuyer.stat.*;
import umontreal.iro.lecuyer.rng.*;
import umontreal.iro.lecuyer.probdist.ExponentialDist;
import umontreal.iro.lecuyer.util.Chrono;

public class QueueLindley {
	RandomStream streamArr = new MRG32k3a();
	RandomStream streamServ = new MRG32k3a();

	Tally averageWaits = new Tally ("Average waits");

	public double simulateOneRun (int numCust, double lambda, double mu) {
		double Wi = 0.0;
		double sumWi = 0.0;

		for (int i = 2; i <= numCust; i++) {
			Wi += ExponentialDist.inverseF (mu, streamServ.nextDouble()) -
					ExponentialDist.inverseF (lambda, streamArr.nextDouble());

			if (Wi < 0.0) 
				Wi = 0.0;
			
			sumWi += Wi;
		}
		
		return sumWi / numCust;
	}

	public void simulateRuns (int n, int numCust, double lambda, double mu) {
		averageWaits.init();
		
		for (int i=0; i<n; i++)
			averageWaits.add (simulateOneRun (numCust, lambda, mu));
	}

	public static void main (String[] args) {
		Chrono timer = new Chrono();
		QueueLindley queue = new QueueLindley();

		queue.simulateRuns (100, 10000, 1.0, 2.0);
		
		System.out.println (queue.averageWaits.report());
		System.out.println ("Total CPU time: " + timer.format());
	}
}