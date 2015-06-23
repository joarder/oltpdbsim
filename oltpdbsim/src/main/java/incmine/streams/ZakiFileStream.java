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

package main.java.incmine.streams;

import java.io.*;
import moa.core.InputStreamProgressMonitor;
import moa.core.InstancesHeader;
import moa.core.ObjectRepository;
import moa.options.AbstractOptionHandler;
import moa.options.FileOption;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;
import weka.core.Instance;
import weka.core.SparseInstance;

/**
 * Reads a stream from a file generated using the IMB-Generator from Zaki.
 * @author Massimo
 */
public class ZakiFileStream extends AbstractOptionHandler 
    implements InstanceStream {

    @Override
    public String getPurposeString() {
        return "A stream read from a file created with Zaki's IBM-Generator.";
    }
    
    private static final long serialVersionUID = 1L;

    public FileOption zakiFileOption = new FileOption("zakiFile", 'f',
            "Zaki file to load.", null, "data", false);

    protected BufferedReader fileReader;

    protected boolean hitEndOfFile;

    protected Instance lastInstanceRead;

    protected int numInstancesRead;

    protected InputStreamProgressMonitor fileProgressMonitor;
    
    public ZakiFileStream(){
    }
    
    public ZakiFileStream(String zakiFileName){
        this.zakiFileOption.setValue(zakiFileName);
        restart();
    }
    
    @Override
    protected void prepareForUseImpl(TaskMonitor tm, ObjectRepository or) {
        restart();
    }

    public void getDescription(StringBuilder sb, int i) {
    }

    public InstancesHeader getHeader() {
        return null;
    }

    public long estimatedRemainingInstances() {
        double progressFraction = this.fileProgressMonitor.getProgressFraction();
        if ((progressFraction > 0.0) && (this.numInstancesRead > 0)) {
            return (long) ((this.numInstancesRead / progressFraction) - this.numInstancesRead);
        }
        return -1;
    }

    public boolean hasMoreInstances() {
        return !this.hitEndOfFile;
    }

    public Instance nextInstance() {
        Instance prevInstance = this.lastInstanceRead;
        this.hitEndOfFile = !readNextInstanceFromFile();
        return prevInstance;
    }

    public boolean isRestartable() {
        return true;
    }

    public void restart() {
        try {
            if (this.fileReader != null) {
                this.fileReader.close();
            }
            InputStream fileStream = new FileInputStream(this.zakiFileOption.getFile());
            this.fileProgressMonitor = new InputStreamProgressMonitor(
                    fileStream);
            this.fileReader = new BufferedReader(new InputStreamReader(
                    this.fileProgressMonitor));
            this.numInstancesRead = 0;
            this.lastInstanceRead = null;
            this.hitEndOfFile = !readNextInstanceFromFile();
        } catch (IOException ioe) {
            throw new RuntimeException("ZakiFileStream restart failed.", ioe);
        }
    }

    protected boolean readNextInstanceFromFile() {
        try {
            String line = this.fileReader.readLine();
            if(line != null){
                String[] lineSplitted = line.split(" ");
                int nItems = Integer.parseInt(lineSplitted[2]);
                double[] attValues = new double[nItems];
                int[] indices = new int[nItems];
                
                for(int idx = 0; idx < nItems; idx++){
                    attValues[idx] = 1;
                    indices[idx] = Integer.parseInt(lineSplitted[idx + 3]);
                }
                
                this.lastInstanceRead = new SparseInstance(1.0,attValues,indices,nItems);
                return true;
            }
            if (this.fileReader != null) {
                this.fileReader.close();
                this.fileReader = null;
            }
            return false;
        } catch (IOException ex) {
            throw new RuntimeException(
                    "ZakiFileStream failed to read instance from stream.", ex);
        }
    }
    

    
}
