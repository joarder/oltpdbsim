/*
 *    Learner.java
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
