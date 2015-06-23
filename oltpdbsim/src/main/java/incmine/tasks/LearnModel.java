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
 * Source: http://www.cs.waikato.ac.nz/~abifet/MOA-IncMine/
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
