package org.wikipedia.miner.extract.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public abstract class LocalStep {

	
	private Path dir ;
	private FileSystem fs ;
	
	public LocalStep(Path dir) throws IOException {
		
		this.dir = dir ;
		fs = dir.getFileSystem(new Configuration()) ;
	}
	
	public abstract int run() throws Exception ;
		
	public boolean isFinished() throws IOException {
		
		return fs.exists(getFinishPath()) ;
	}
	
	public void finish() throws IOException {
		
		FSDataOutputStream out = fs.create(getFinishPath());
		
		out.writeUTF("finished") ;
		
		out.close();
	}
	
	public void reset() throws IOException {
		
		fs.delete(getDir(), true) ;
	}
	
	public FileSystem getFs() {
		return fs ;
	}
	
	public Path getDir() {
		return dir ;
	}
	
	
	private Path getFinishPath() {		
		return new Path(getDir().toString() + Path.SEPARATOR + "finished") ;
	}	
	
}
