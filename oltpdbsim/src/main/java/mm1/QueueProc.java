/**
 * Process-oriented simulation of an M/M/1 queue
 * 
 * The process-oriented program here is more compact and more elegant than its event-
 * oriented counterpart. This tends to be often true: Process-oriented programming frequently
 * gives less cumbersome and better looking programs. On the other hand, the process-oriented
 * implementations also tend to execute more slowly, because they involve more overhead. For
 * example, the process-driven single-server queue simulation is two to three times slower than
 * its event-driven counterpart. In fact, process management is done via the event list: processes
 * are started, suspended, reactivated, and so on, by hidden events. If the execution speed of
 * a simulation program is really important, it may be better to stick to an event-oriented
 * implementation.
 */

package main.java.mm1;

import umontreal.iro.lecuyer.simevents.*;
import umontreal.iro.lecuyer.simprocs.*;
import umontreal.iro.lecuyer.rng.*;
import umontreal.iro.lecuyer.randvar.*;

public class QueueProc {
	Resource server = new Resource (1, "Server");
	
	RandomVariateGen genArr;
	RandomVariateGen genServ;

	public QueueProc (double lambda, double mu) {
		genArr = new ExponentialGen (new MRG32k3a(), lambda);
		genServ = new ExponentialGen (new MRG32k3a(), mu);
	}

	public void simulateOneRun (double timeHorizon) {
		SimProcess.init();
		server.setStatCollecting (true);
		new EndOfSim().schedule (timeHorizon);
		new Customer().schedule (genArr.nextDouble());
		Sim.start();
	}

	class Customer extends SimProcess {
		public void actions() {
			new Customer().schedule (genArr.nextDouble());
			server.request (1);
			delay (genServ.nextDouble());
			server.release (1);
		}
	}	
	
	class EndOfSim extends Event {
		public void actions() { 
			Sim.stop(); 
		}
	}

	public static void main (String[] args) {
		QueueProc queue = new QueueProc (1.0, 2.0);
		queue.simulateOneRun (1000.0);
	
		System.out.println (queue.server.report());
	}
}