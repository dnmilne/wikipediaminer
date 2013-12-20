package org.wikipedia.miner.extract.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;

public abstract class Step extends Configured implements Tool {

	private Counters counters ;

	private Path workingDir ;
	private FileSystem hdfs ;
	
	public Step(Path workingDir) throws IOException {
		
		this.workingDir = workingDir ;
		
		Configuration conf = new Configuration();
		hdfs = FileSystem.get(conf);
	}
	
	public Counters getCounters() {
		return counters ;
	}
	

	
	
	public boolean isFinished() throws IOException {
		
		return hdfs.exists(getFinishPath()) ;
	}
	
	public void finish(RunningJob job) throws IOException {
		
		
		if (job != null)
			counters = job.getCounters() ;
		
		FSDataOutputStream out = hdfs.create(getFinishPath());
		
		out.writeUTF("finished") ;
		
		out.close();
	}
	
	public void reset() throws IOException {
		
		hdfs.delete(getDir(), true) ;
	}
	
	public FileSystem getHdfs() {
		return hdfs ;
	}
	
	public Path getWorkingDir() {
		return workingDir ;
	}
	
	public Path getDir() {
		return new Path(workingDir.toString() + Path.SEPARATOR + getDirName()) ;
	}
	
	private Path getFinishPath() {		
		return new Path(getDir().toString() + Path.SEPARATOR + "finished") ;
	}
	
	public abstract String getDirName() ;
	
	
	
}
