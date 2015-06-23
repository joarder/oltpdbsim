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

package main.java.incmine.evaluation;

import java.util.LinkedList;
import java.util.List;
import moa.AbstractMOAObject;
import moa.classifiers.Classifier;
import moa.clusterers.Clusterer;
import main.java.incmine.learners.Learner;
import moa.core.Measurement;
import moa.evaluation.ClassificationPerformanceEvaluator;
import moa.evaluation.LearningPerformanceEvaluator;

public class LearningEvaluation extends AbstractMOAObject {

    private static final long serialVersionUID = 1L;

    protected Measurement[] measurements;

    public LearningEvaluation(Measurement[] measurements) {
        this.measurements = measurements.clone();
    }

    public LearningEvaluation(Measurement[] evaluationMeasurements,
            ClassificationPerformanceEvaluator cpe, Classifier model) {
        List<Measurement> measurementList = new LinkedList<Measurement>();
        for (Measurement measurement : evaluationMeasurements) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : cpe.getPerformanceMeasurements()) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : model.getModelMeasurements()) {
            measurementList.add(measurement);
        }
        this.measurements = measurementList.toArray(new Measurement[measurementList.size()]);
    }

    // Must change to Learner model
    public LearningEvaluation(Measurement[] evaluationMeasurements,
            LearningPerformanceEvaluator cpe, Clusterer model) {
        List<Measurement> measurementList = new LinkedList<Measurement>();
        for (Measurement measurement : evaluationMeasurements) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : cpe.getPerformanceMeasurements()) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : model.getModelMeasurements()) {
            measurementList.add(measurement);
        }
        this.measurements = measurementList.toArray(new Measurement[measurementList.size()]);
    }

    public LearningEvaluation(Measurement[] evaluationMeasurements,
            ClassificationPerformanceEvaluator cpe, Learner model) {
        List<Measurement> measurementList = new LinkedList<Measurement>();
        for (Measurement measurement : evaluationMeasurements) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : cpe.getPerformanceMeasurements()) {
            measurementList.add(measurement);
        }
        for (Measurement measurement : model.getModelMeasurements()) {
            measurementList.add(measurement);
        }
        this.measurements = measurementList.toArray(new Measurement[measurementList.size()]);
    }

    public Measurement[] getMeasurements() {
        return this.measurements.clone();
    }

    public void getDescription(StringBuilder sb, int indent) {
        Measurement.getMeasurementsDescription(this.measurements, sb, indent);
    }
}
