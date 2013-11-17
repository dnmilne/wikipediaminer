package org.wikipedia.miner.extract.util;
import java.util.*;
import java.io.*;

public class HadoopConfigurer {
	
	
	private ArrayList<String> nodes = new ArrayList<String>() ;
	private File installDir ;
	private File confDir ;
	private File workingDir ;
	
	
	public HadoopConfigurer(String[] args) throws IOException {
		
		setNodes(new File(args[0])) ;
		
		installDir = new File(args[1]) ;
		
		confDir = new File(installDir + "/conf") ;
		if (!confDir.isDirectory() || !confDir.canWrite()) 
			throw new IOException("'" + confDir + "' is not a writable directory!") ;		
		
		workingDir = new File(args[2]) ;
		if (workingDir.exists() && (!workingDir.isDirectory() || !workingDir.canWrite())) 
			throw new IOException("'" + workingDir + "' exits but is not a writable directory!") ;
		
	}
	
	public void doConfiguration() throws IOException {
		
		configureMasters() ;
		configureSlaves() ;
		configureCoreSite() ;
		configureHdfsSite() ;
		configureMapredSite() ;
		
		System.out.println("Hadoop installation at '" + installDir + "' has been configured.") ;
		System.out.println() ;
		System.out.println("Web UIs will be available at:") ;
		System.out.println(" - NameNode: http://" + nodes.get(0) + ":50070") ;
		System.out.println(" - JobTracker: http://" + nodes.get(0) + ":50030") ;
		
	}
	
	public static void main(String args[]) {
		
		if (args.length != 3) {
			
			System.err.println("FATAL: HadoopConfigurer: invalid arguments") ;
			System.exit(9) ;
		}
			
		try {
			HadoopConfigurer hc = new HadoopConfigurer(args) ;
			hc.doConfiguration() ;
		} catch (Exception e) {
			System.out.println("FATAL: HadoopConfigurer: could not complete configuration") ;
			e.printStackTrace() ;
			System.exit(9) ;
		}
	}
	
	
	private void setNodes(File nodeFile) throws IOException {
		
		nodes = new ArrayList<String>() ; 
		
		BufferedReader input=new BufferedReader(new FileReader(nodeFile));
		
		String line ;
		while((line=input.readLine())!=null){			
			nodes.add(line.trim()) ;
		}
	}
	
	private void configureMasters() throws IOException {
		File masterFile = new File(confDir.getAbsolutePath() + "/masters") ;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(masterFile)) ; 
		writer.write(nodes.get(0) + "\n") ;
		writer.close() ;
	}
	
	private void configureSlaves() throws IOException {
		File slaveFile = new File(confDir.getAbsolutePath() + "/slaves") ;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(slaveFile)) ; 
		for (int i=1 ; i<nodes.size(); i++)
			writer.write(nodes.get(i) + "\n") ;
		writer.close() ;
	}
	
	private void configureCoreSite() throws IOException {
		
		StringBuffer conf = new StringBuffer() ;
		
		writeConfigHeader(conf) ;
		startConfig(conf) ;
		
		writeProperty(conf, "fs.default.name", "hdfs://" + nodes.get(0) + ":9000") ;
		
		endConfig(conf) ;
		
		File confFile = new File(confDir.getAbsolutePath() + "/core-site.xml") ;
		
		FileWriter writer = new FileWriter(confFile) ; 
		writer.write(conf.toString()) ;
		writer.close() ;
	}
	
	private void configureHdfsSite() throws IOException {

		StringBuffer conf = new StringBuffer() ;
		
		writeConfigHeader(conf) ;
		startConfig(conf) ;
		writeProperty(conf, "dfs.name.dir", workingDir.getAbsolutePath() + "/log") ;
		writeProperty(conf, "dfs.name.dir", workingDir.getAbsolutePath() + "/data") ;
		endConfig(conf) ;
		
		File confFile = new File(confDir + "/hdfs-site.xml") ;
		
		FileWriter writer = new FileWriter(confFile) ; 
		writer.write(conf.toString()) ;
		writer.close() ;
	}
	
	private void configureMapredSite() throws IOException {
				
		StringBuffer conf = new StringBuffer() ;
		
		writeConfigHeader(conf) ;
		startConfig(conf) ;
		writeProperty(conf, "mapred.job.tracker", nodes.get(0) + ":9001") ;
		writeProperty(conf, "mapred.system.dir", "/hadoop/mapred/system") ;
		writeProperty(conf, "mapred.local.dir", workingDir.getAbsolutePath() + "/tmp1," + workingDir.getAbsolutePath() + "/tmp2") ;
		writeProperty(conf, "mapred.queue.names", "default") ;
		endConfig(conf) ;
		
		File confFile = new File(confDir + "/mapred-site.xml") ;
		
		FileWriter writer = new FileWriter(confFile) ; 
		writer.write(conf.toString()) ;
		writer.close() ;
	}
	
	
	
	
	
	
	private void writeConfigHeader(StringBuffer conf) {
		conf.append("<?xml version=\"1.0\"?>\n") ;
		conf.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n") ;
		conf.append("\n") ;
	}
	
	private void startConfig(StringBuffer conf) {
		conf.append("<configuration>\n") ;
	}
	
	private void endConfig(StringBuffer conf) {
		conf.append("</configuration>\n") ;
	}
	
	private void writeProperty(StringBuffer conf, String name, String value) {
		conf.append("  <property>\n") ;
		conf.append("    <name>" + name + "</name>\n") ;
		conf.append("    <value>" + value + "</value>\n") ; 
		conf.append("  </property>\n") ;		
	}
}
