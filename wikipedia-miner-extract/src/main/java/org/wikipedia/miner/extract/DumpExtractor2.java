package org.wikipedia.miner.extract;


import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TIntShortHashMap;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.*;
import org.wikipedia.miner.db.struct.*;
import org.wikipedia.miner.extract.steps.*;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.model.struct.ExLabel;
import org.wikipedia.miner.extract.model.struct.ExSenseForLabel;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.util.ProgressTracker;

import steps2.InitialPageSummaryStep;

import com.sleepycat.je.dbi.StartupTracker.Counter;



/**
 * @author dnk2
 *
 * This class extracts summaries (link graphs, etc) from Wikipedia xml dumps. 
 * It calls a sequence of Hadoop Map/Reduce jobs to do so in a scalable, timely fashion.
 * 
 * 
 *  
 */
@SuppressWarnings("deprecation")
public class DumpExtractor2 {

	private Configuration conf ;

	private String[] args ;

	private Path inputFile ;
	private Path langFile ;
	private String lang ;
	private Path sentenceModel ;
	private Path workingDir  ;
	private Path finalDir ;

	private LanguageConfiguration lc ;
	//private Logger logger ;


	public static final String KEY_INPUT_FILE = "wm.inputDir" ;
	public static final String KEY_OUTPUT_DIR = "wm.workingDir" ;
	public static final String KEY_LANG_FILE = "wm.langFile" ;
	public static final String KEY_LANG_CODE = "wm.langCode" ;
	public static final String KEY_SENTENCE_MODEL = "wm.sentenceModel" ;

	public static final String LOG_ORPHANED_PAGES = "orphanedPages" ;
	public static final String LOG_WEIRD_LABEL_COUNT = "wierdLabelCounts" ;
	public static final String LOG_MEMORY_USE = "memoryUsage" ;


	public static final String OUTPUT_SITEINFO = "final/siteInfo.xml" ;
	public static final String OUTPUT_PROGRESS = "tempProgress.csv" ;
	public static final String OUTPUT_TEMPSTATS = "tempStats.csv" ;
	public static final String OUTPUT_STATS = "final/stats.csv" ;



	public DumpExtractor2(String[] args) throws Exception {


		GenericOptionsParser gop = new GenericOptionsParser(args) ;
		conf = gop.getConfiguration() ;



		//outputFileSystem = FileSystem.get(conf);
		this.args = gop.getRemainingArgs() ;

		configure() ;
	}

	public static void main(String[] args) throws Exception {

		//PropertyConfigurator.configure("log4j.properties");  

		DumpExtractor2 de = new DumpExtractor2(args) ;
		int result = de.run();

		System.exit(result) ;
	}

	public static JobConf configureJob(JobConf conf, String[] args) {

		conf.set(KEY_INPUT_FILE, args[0]) ;
		conf.set(KEY_LANG_FILE, args[1]) ;
		conf.set(KEY_LANG_CODE, args[2]) ;
		conf.set(KEY_SENTENCE_MODEL, args[3]) ;
		conf.set(KEY_OUTPUT_DIR, args[4]) ;

		//force one reducer. These don't take very long, and multiple reducers would make finalise file functions more complicated.  

		conf.setNumMapTasks(64) ;
		conf.setNumReduceTasks(1) ;

		//many of our tasks require pre-loading lots of data, may as well reuse this as much as we can.
		//conf.setNumTasksToExecutePerJvm(-1) ;



		//conf.setInt("mapred.tasktracker.map.tasks.maximum", 2) ;
		//conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1) ;
		//conf.set("mapred.child.java.opts", "-Xmx3500M") ;

		//conf.setBoolean("mapred.used.genericoptionsparser", true) ;

		return conf ;
	}



	private FileSystem getFileSystem(Path path) throws IOException {
		return path.getFileSystem(conf) ;
	}


	private Path getPath(String pathStr) {
		return new Path(pathStr) ;
	}


	private FileStatus getFileStatus(Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		return fs.getFileStatus(path) ;
	}







	private void configure() throws Exception {

		if (args.length != 6) 
			throw new IllegalArgumentException("Please specify a xml dump of wikipedia, a language.xml config file, a language code, an openNLP sentence detection model, an hdfs writable working directory, and an output directory") ;


		//check input file
		inputFile = getPath(args[0]); 
		FileStatus fs = getFileStatus(inputFile) ;
		if (fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.READ)) 
			throw new IOException("'" +inputFile + " is not readable or does not exist") ;


		//check lang file and language
		langFile = getPath(args[1]) ;
		lang = args[2] ;
		lc = new LanguageConfiguration(langFile.getFileSystem(conf), lang, langFile) ;
		if (lc == null)
			throw new IOException("Could not load language configuration for '" + lang + "' from '" + langFile + "'") ;

		sentenceModel = new Path(args[3]) ;
		fs = getFileStatus(sentenceModel) ;
		if (fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.READ)) 
			throw new IOException("'" + sentenceModel + " is not readable or does not exist") ;

		//check working directory
		workingDir = new Path(args[4]) ;

		if (!getFileSystem(workingDir).exists(workingDir))
			getFileSystem(workingDir).mkdirs(workingDir) ;

		fs = getFileStatus(workingDir) ;
		if (!fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.WRITE)) 
			throw new IOException("'" +workingDir + " is not a writable directory") ;

		//set up directory where final data will be placed
		finalDir = new Path(args[5]) ;

		if (getFileSystem(finalDir).exists(finalDir))
			getFileSystem(finalDir).delete(finalDir, true) ;

		getFileSystem(finalDir).mkdirs(finalDir) ;

		fs = getFileStatus(finalDir) ;
		if (!fs.isDir() || !fs.getPermission().getUserAction().implies(FsAction.WRITE)) 
			throw new IOException("'" +workingDir + " is not a writable directory") ;

	}

	private int run() throws Exception {

		FileSystem fs = getFileSystem(workingDir) ;

		Logger.getLogger(DumpExtractor2.class).info("Extracting site info") ;
		extractSiteInfo() ;

		int result = 0 ;

		DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss") ;

		String currStep = "page" ;

		Logger.getLogger(DumpExtractor2.class).info("Starting " + currStep + " step") ;
		//fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

		long startTime = System.currentTimeMillis() ;

		InitialPageSummaryStep step = new InitialPageSummaryStep(workingDir) ;

		result = ToolRunner.run(new Configuration(), step, args);
		if (result != 0) {
			Logger.getLogger(DumpExtractor2.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
			return result ;
		}

		long unforwardedRedirects = step.getCounters().findCounter(InitialPageSummaryStep.Counter.unforwardedRedirectCount).getCounter() ;

		System.out.println("unforwardedRedirects: " + unforwardedRedirects) ;

		//update statistics
		//stats = step.updateStats(stats) ;
		//stats.put("lastEdit", getLastEdit()) ;
		//writeStatistics(stats) ;

		//update progress
		//lastCompletedStep = currStep ;
		//writeProgress(lastCompletedStep) ;

		//print time
		System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;

		return result ;
	}




	private void extractSiteInfo() throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(inputFile).open(inputFile))) ;
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(workingDir).create(new Path(workingDir + "/" + OUTPUT_SITEINFO)))) ;

		String line = null;
		boolean startedWriting = false ;

		while ((line = reader.readLine()) != null) {

			if (!startedWriting && line.matches("\\s*\\<siteinfo\\>\\s*")) 
				startedWriting = true ;

			if (startedWriting) {
				writer.write(line) ;
				writer.newLine() ;

				if (line.matches("\\s*\\<\\/siteinfo\\>\\s*"))
					break ;
			}
		}

		reader.close() ;
		writer.close();
	}



}
