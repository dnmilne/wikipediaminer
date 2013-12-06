package steps2;

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

	private Path baseWorkingDir ;
	private FileSystem hdfs ;
	
	public Step(Path baseWorkingDir) throws IOException {
		
		this.baseWorkingDir = baseWorkingDir ;
		
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
		
		counters = job.getCounters() ;
		
		FSDataOutputStream out = hdfs.create(getFinishPath());
		
		counters.write(out) ;
		
		out.close();
	}
	
	public void reset() throws IOException {
		
		hdfs.delete(getWorkingDir(), true) ;
	}
	
	public Path getBaseWorkingDir() {
		return baseWorkingDir ;
	}
	
	public Path getWorkingDir() {
		return new Path(baseWorkingDir.toString() + Path.SEPARATOR + getWorkingDirName()) ;
	}
	
	private Path getFinishPath() {		
		return new Path(getWorkingDir().toString() + Path.SEPARATOR + "finished") ;
	}
	
	public abstract String getWorkingDirName() ;
	
	
	
}
