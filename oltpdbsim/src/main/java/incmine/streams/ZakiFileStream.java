/*
 *    ZakiFileStream.java
 *    Copyright (C) 2012 Universitat Politècnica de Catalunya
 *    @author Massimo Quadrana <max.square@gmail.com>
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
