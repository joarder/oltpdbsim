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

package main.java.incmine.learners;

import moa.MOAObject;
import moa.core.InstancesHeader;
import moa.core.Measurement;
import moa.gui.AWTRenderable;
import moa.options.OptionHandler;
import weka.core.Instance;

public interface Learner extends MOAObject, OptionHandler, AWTRenderable {

    public void setModelContext(InstancesHeader ih);

    public InstancesHeader getModelContext();

    public boolean isRandomizable();

    public void setRandomSeed(int s);

    public boolean trainingHasStarted();

    public double trainingWeightSeenByModel();

    public void resetLearning();

    public void trainOnInstance(Instance instance); //Changed Instance to Example

    public Measurement[] getModelMeasurements();

    public Learner[] getSubLearners();

    public Learner copy();

    public MOAObject getModel();
}
