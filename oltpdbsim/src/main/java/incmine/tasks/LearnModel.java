/*
 *    LearnModel.java
 *    Copyright (C) 2011 University of Waikato, Hamilton, New Zealand
 *    @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 *    @author Albert Bifet (abifet at cs dot waikato dot ac dot nz)
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
package main.java.incmine.tasks;

import moa.core.ObjectRepository;
import main.java.incmine.learners.Learner;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.streams.InstanceStream;
import moa.tasks.MainTask;
import moa.tasks.TaskMonitor;

public abstract class LearnModel extends MainTask {

    @Override
    public String getPurposeString() {
        return "Learns a model from a stream.";
    }
    private static final long serialVersionUID = 1L;

    public ClassOption learnerOption = new ClassOption("learner", 'l',
            "Classifier to train.", Learner.class, "IncMine");

    public ClassOption streamOption = new ClassOption("stream", 's',
            "Stream to learn from.", InstanceStream.class, "ArffFileStream");

    public IntOption maxInstancesOption = new IntOption("maxInstances", 'm',
            "Maximum number of instances to train on per pass over the data.",
            1000, 0, Integer.MAX_VALUE);

    public IntOption numPassesOption = new IntOption("numPasses", 'p',
            "The number of passes to do over the data.", 1, 1,
            Integer.MAX_VALUE);

    public IntOption maxMemoryOption = new IntOption("maxMemory", 'b',
            "Maximum size of model (in bytes). -1 = no limit.", -1, -1,
            Integer.MAX_VALUE);

    public LearnModel() {
    }

    public LearnModel(Learner learner, InstanceStream stream,
            int maxInstances, int numPasses) {
        this.learnerOption.setCurrentObject(learner);
        this.streamOption.setCurrentObject(stream);
        this.maxInstancesOption.setValue(maxInstances);
        this.numPassesOption.setValue(numPasses);
    }

    public Class<?> getTaskResultType() {
        return Learner.class;
    }

    @Override
    public Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
        Learner learner = (Learner) getPreparedClassOption(this.learnerOption);
        InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
        learner.setModelContext(stream.getHeader());
        int numPasses = this.numPassesOption.getValue();
        int maxInstances = this.maxInstancesOption.getValue();
        for (int pass = 0; pass < numPasses; pass++) {
            long instancesProcessed = 0;
            monitor.setCurrentActivity("Training learner"
                    + (numPasses > 1 ? (" (pass " + (pass + 1) + "/"
                    + numPasses + ")") : "") + "...", -1.0);
            /*if (pass > 0) {
            stream.restart();
            }*/
            while (stream.hasMoreInstances()
                    && ((maxInstances < 0) || (instancesProcessed < maxInstances))) {
                // Read instance one by one
                learner.trainOnInstance(stream.nextInstance());
                instancesProcessed++;
                if (instancesProcessed % INSTANCES_BETWEEN_MONITOR_UPDATES == 0) {
                    //System.out.println("inst_betw_mon_upd = 0 instProcess" + instancesProcessed);
                    if (monitor.taskShouldAbort()) {
                        return null;
                    }
                    long estimatedRemainingInstances = stream.estimatedRemainingInstances();
                    if (maxInstances > 0) {
                        long maxRemaining = maxInstances - instancesProcessed;
                        if ((estimatedRemainingInstances < 0)
                                || (maxRemaining < estimatedRemainingInstances)) {
                            estimatedRemainingInstances = maxRemaining;
                        }
                    }
                    monitor.setCurrentActivityFractionComplete(estimatedRemainingInstances < 0 ? -1.0
                            : (double) instancesProcessed
                            / (double) (instancesProcessed + estimatedRemainingInstances));
                    if (monitor.resultPreviewRequested()) {
                        monitor.setLatestResultPreview(learner.copy());
                    }
                }
            }
        }
        learner.setModelContext(stream.getHeader());
        return learner;
    }
}
