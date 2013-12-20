package org.wikipedia.miner.extract.steps;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public abstract class IterativeStep extends Step{

	private int iteration ;
	
	public IterativeStep(Path workingDir, int iteration) throws IOException {
		super(workingDir);
		
		this.iteration = iteration ;
	}
	
	public int getIteration() {
		return iteration ;
	}
	
	public abstract String getDirName(int iteration) ;
	
	public String getDirName() {
		return getDirName(iteration) ;
	}
	
	public abstract boolean furtherIterationsRequired() ;

}
