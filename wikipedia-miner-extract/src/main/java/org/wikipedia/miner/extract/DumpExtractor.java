package org.wikipedia.miner.extract;


import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.* ;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
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
public class DumpExtractor {

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

	public enum ExtractionStep {
		page, redirect, labelSense, pageLabel, labelOccurrence, pageLink, categoryParent, articleParent, linkCooccurrence, relatedness 	
	}


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



	public DumpExtractor(String[] args) throws Exception {


		GenericOptionsParser gop = new GenericOptionsParser(args) ;
		conf = gop.getConfiguration() ;
		
		
		
		//outputFileSystem = FileSystem.get(conf);
		this.args = gop.getRemainingArgs() ;

		configure() ;
		configureLogging() ;
	}

	public static void main(String[] args) throws Exception {

		//PropertyConfigurator.configure("log4j.properties");  

		DumpExtractor de = new DumpExtractor(args) ;
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
		conf.setNumTasksToExecutePerJvm(-1) ;
		
		conf.setInt("mapred.tasktracker.map.tasks.maximum", 4) ;
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1) ;
		conf.set("mapred.child.java.opts", "-Xmx1750M") ;

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

		//check output directory
		workingDir = new Path(args[4]) ;
		
		
		//TODO: this should be dependent on an "overwrite" flag
		//if (getFileSystem(workingDir).exists(workingDir))
		//	getFileSystem(workingDir).delete(workingDir, true) ;

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

	private void configureLogging() throws IOException {
		
		FileSystem fs = getFileSystem(workingDir) ;
		
		Path logDir = new Path(workingDir + "/logs") ;
		fs.mkdirs(logDir) ;

		Logger logger ; 

		logger = Logger.getLogger(DumpExtractor.LOG_ORPHANED_PAGES) ;
		logger.setAdditivity(false);
		logger.addAppender(new WriterAppender(new PatternLayout("%-5p: %m%n"), new OutputStreamWriter(fs.create(new Path(logDir + "/" + DumpExtractor.LOG_ORPHANED_PAGES + ".log"))))) ;

		logger = Logger.getLogger(DumpExtractor.LOG_WEIRD_LABEL_COUNT) ;
		logger.setAdditivity(false);
		logger.addAppender(new WriterAppender(new PatternLayout("%-5p: %m%n"), new OutputStreamWriter(fs.create(new Path(logDir + "/" + DumpExtractor.LOG_WEIRD_LABEL_COUNT + ".log"))))) ;

		logger = Logger.getLogger(DumpExtractor.LOG_MEMORY_USE) ;
		logger.setAdditivity(false);
		logger.addAppender(new WriterAppender(new PatternLayout("%-5p: %m%n"), new OutputStreamWriter(fs.create(new Path(logDir + "/" + DumpExtractor.LOG_MEMORY_USE + ".log"))))) ;
	}

	private int run() throws Exception {

		FileSystem fs = getFileSystem(workingDir) ;

		Logger.getLogger(DumpExtractor.class).info("Extracting site info") ;
		extractSiteInfo() ;

		int result = 0 ;

		ExtractionStep lastCompletedStep = readProgress() ;		
		TreeMap<String,Long> stats ;

		if (lastCompletedStep != null)
			stats = readStatistics() ;
		else
			stats = new TreeMap<String, Long>() ;

		DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss") ;

		if (lastCompletedStep == null) {


			ExtractionStep currStep = ExtractionStep.page ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			PageStep step = new PageStep() ;

			result = ToolRunner.run(new Configuration(), step, args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			//update statistics
			stats = step.updateStats(stats) ;
			stats.put("lastEdit", getLastEdit()) ;
			writeStatistics(stats) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}


		if (lastCompletedStep.compareTo(ExtractionStep.redirect) < 0) {
			ExtractionStep currStep = ExtractionStep.redirect ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			RedirectStep step = new RedirectStep() ;
			result = ToolRunner.run(new Configuration(), step, args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			//finalize redirect files
			finalizeFile(ExtractionStep.redirect, RedirectStep.Output.redirectSourcesByTarget.name()) ;
			finalizeFile(ExtractionStep.redirect, RedirectStep.Output.redirectTargetsBySource.name()) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}


		if (lastCompletedStep.compareTo(ExtractionStep.labelSense) < 0) {
			ExtractionStep currStep = ExtractionStep.labelSense ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			LabelSensesStep step = new LabelSensesStep() ;
			result = ToolRunner.run(new Configuration(), step, args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeFile(currStep, LabelSensesStep.Output.sentenceSplits.name()) ;
			finalizeFile(currStep, LabelSensesStep.Output.translations.name()) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}

		if (lastCompletedStep.compareTo(ExtractionStep.pageLabel) < 0) {
			ExtractionStep currStep = ExtractionStep.pageLabel ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			PageLabelStep step = new PageLabelStep() ;
			result = ToolRunner.run(new Configuration(), step, args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeFile(currStep, PageLabelStep.Output.pageLabel.name()) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}

		if (lastCompletedStep.compareTo(ExtractionStep.labelOccurrence) < 0) {
			ExtractionStep currStep = ExtractionStep.labelOccurrence ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			LabelOccurrencesStep step = new LabelOccurrencesStep() ;
			result = ToolRunner.run(new Configuration(), step, args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeLabels() ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}

		if (lastCompletedStep.compareTo(ExtractionStep.pageLink) < 0) {
			ExtractionStep currStep = ExtractionStep.pageLink ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			result = ToolRunner.run(new Configuration(), new PageLinkSummaryStep(), args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeFile(currStep, PageLinkSummaryStep.Output.pageLinkIn.name()) ;
			finalizeFile(currStep, PageLinkSummaryStep.Output.pageLinkOut.name()) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}

		if (lastCompletedStep.compareTo(ExtractionStep.categoryParent) < 0) {
			ExtractionStep currStep = ExtractionStep.categoryParent ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			result = ToolRunner.run(new Configuration(), new CategoryLinkSummaryStep(currStep), args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeFile(currStep, CategoryLinkSummaryStep.Output.categoryParents.name()) ;
			finalizeFile(currStep, CategoryLinkSummaryStep.Output.childCategories.name()) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}


		if (lastCompletedStep.compareTo(ExtractionStep.articleParent) < 0) {
			ExtractionStep currStep = ExtractionStep.articleParent ;
			Logger.getLogger(DumpExtractor.class).info("Starting " + currStep + " step") ;
			fs.delete(new Path(workingDir + "/" + getDirectoryName(currStep)), true) ;

			long startTime = System.currentTimeMillis() ;

			result = ToolRunner.run(new Configuration(), new CategoryLinkSummaryStep(currStep), args);
			if (result != 0) {
				Logger.getLogger(DumpExtractor.class).fatal("Could not complete " + currStep + " step. Check map/reduce user logs for an explanation.") ;
				return result ;
			}

			finalizeFile(currStep, CategoryLinkSummaryStep.Output.articleParents.name()) ;
			finalizeFile(currStep, CategoryLinkSummaryStep.Output.childArticles.name()) ;


			finalizePages(stats) ;
			finalizeStatistics(stats) ;

			//update progress
			lastCompletedStep = currStep ;
			writeProgress(lastCompletedStep) ;

			//print time
			System.out.println(currStep + " step completed in " + timeFormat.format(System.currentTimeMillis()-startTime)) ;
		}

		return result ;
	}

	private ExtractionStep readProgress() {

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(new Path(workingDir + "/" + OUTPUT_PROGRESS)))) ;

			int step = reader.read();
			reader.close();

			return ExtractionStep.values()[step] ;

		} catch (IOException e) {
			return null ;
		}

	}

	private void writeProgress(ExtractionStep lastCompletedStep) throws IOException {

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(workingDir).create(new Path(workingDir + "/" + OUTPUT_PROGRESS)))) ;

		writer.write(lastCompletedStep.ordinal()) ;
		writer.close();
	}

	private TreeMap<String, Long> readStatistics() throws IOException {

		TreeMap<String, Long> stats = new TreeMap<String, Long>() ;

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(new Path(workingDir + "/" + OUTPUT_TEMPSTATS)))) ;

			String line ;
			while ((line=reader.readLine()) != null) {

				CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

				String statName = cri.readString(null) ;
				Long statValue = cri.readLong(null) ;		

				stats.put(statName, statValue) ;
			}

			reader.close();
		} catch (IOException e){

		}

		return stats ;
	}

	private void writeStatistics(TreeMap<String, Long> stats) throws IOException {

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(workingDir).create(new Path(workingDir + "/" + OUTPUT_TEMPSTATS)))) ;

		for(Map.Entry<String,Long> e:stats.entrySet()) {

			ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

			CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
			cro.writeString(e.getKey(), null) ;
			cro.writeLong(e.getValue(), null) ;

			writer.write(outStream.toString("UTF-8")) ;
			writer.newLine() ;
		}

		writer.close();
	}

	private HashMap<Integer, Short> calculatePageDepths(TreeMap<String, Long> stats, TIntObjectHashMap<TIntArrayList> childCategories, TIntObjectHashMap<TIntArrayList> childArticles) {

		final HashMap<Integer, Short> pageDepths = new HashMap<Integer, Short>() ;

		Short currDepth = 0 ;
		Integer currCat = stats.get(PageStep.Counter.rootCategoryId.name()).intValue() ;

		Vector<Integer> currLevel = new Vector<Integer>() ;
		Vector<Integer> nextLevel = new Vector<Integer>() ;

		while (currCat != null){

			if (!pageDepths.containsKey(currCat)) {

				//save this categories depth
				pageDepths.put(currCat, currDepth) ;

				//save depths for this categories child articles
				TIntArrayList childArts = childArticles.get(currCat) ;

				if (childArts != null) {
					for(int i=0 ; i<childArts.size() ; i++) {
						Integer childArt = childArts.get(i) ;

						if (!pageDepths.containsKey(childArt)) {					
							pageDepths.put(childArt, (short)(currDepth + 1)) ;
						}
					}
				}

				//push child categories on to stack to process
				TIntArrayList childCats = childCategories.get(currCat) ;
				if (childCats != null) {

					for(int i=0 ; i<childCats.size() ; i++) {
						Integer childCat = childCats.get(i) ;

						if (!pageDepths.containsKey(childCat))
							nextLevel.add(childCat) ;
					}
				}
			}

			if (currLevel.isEmpty()) {

				currLevel = nextLevel ;
				nextLevel = new Vector<Integer>() ;

				currDepth++ ;
			}

			if (currLevel.isEmpty()) {
				currCat = null ;
			} else {
				currCat = currLevel.firstElement() ;
				currLevel.removeElementAt(0) ;
			}
		}

		stats.put("maxCategoryDepth", (long)currDepth) ;

		return pageDepths ;
	}


	private TIntObjectHashMap<TIntArrayList> gatherChildren(ExtractionStep step, final String filePrefix) throws IOException  {

		FileSystem fs = getFileSystem(workingDir) ;
		
		TIntObjectHashMap<TIntArrayList> children = new TIntObjectHashMap<TIntArrayList>() ;


		FileStatus[] fileStatuses = fs.listStatus(new Path(workingDir + "/" + getDirectoryName(step)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(filePrefix) ;
			}
		}) ;

		for (FileStatus status:fileStatuses) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()))) ;

			String line = null;
			while ((line = reader.readLine()) != null) {

				CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

				int parentId = cri.readInt("parent") ;
				DbIntList childIds = new DbIntList() ;
				childIds.deserialize(cri) ;

				if (childIds.getValues() != null && !childIds.getValues().isEmpty()) {
					TIntArrayList cIds = new TIntArrayList() ;
					for (Integer childId:childIds.getValues()) 
						cIds.add(childId) ;

					children.put(parentId, cIds) ;
				}
			}
		}

		return children ;
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


	private void finalizePages(TreeMap<String, Long> stats) throws IOException {
		
		//FileSystem fs = getFileSystem(workingDir) ;
		

		TIntObjectHashMap<TIntArrayList> childCategories = gatherChildren(ExtractionStep.categoryParent, CategoryLinkSummaryStep.Output.childCategories.name()) ;
		TIntObjectHashMap<TIntArrayList> childArticles = gatherChildren(ExtractionStep.articleParent, CategoryLinkSummaryStep.Output.childArticles.name()) ;

		HashMap<Integer, Short> pageDepths = calculatePageDepths(stats, childCategories, childArticles) ;

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(finalDir).create(new Path(finalDir + "/page.csv")))) ;

		FileStatus[] fileStatuses = getFileSystem(workingDir).listStatus(new Path(workingDir + "/" + getDirectoryName(ExtractionStep.page)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(PageStep.Output.tempPage.name()) ;
			}
		}) ;

		for (FileStatus status:fileStatuses) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(status.getPath()))) ;

			String line = null;
			while ((line = reader.readLine()) != null) {

				CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

				int pageId = cri.readInt("id") ;
				DbPage page = new DbPage() ;
				page.deserialize(cri) ;

				PageType pageType = PageType.values()[page.getType()] ;
				Short pageDepth = pageDepths.get(pageId) ;

				if (pageDepth != null) { 
					page.setDepth(pageDepth.intValue()) ;
				} else {
					if (pageType != PageType.redirect)
						Logger.getLogger(DumpExtractor.LOG_ORPHANED_PAGES).warn("Could not identify depth of page " + pageId + ":" + page.getTitle() + "[" + pageType + "]") ;
				}

				ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

				CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
				cro.writeInt(pageId, "id") ;
				page.serialize(cro) ;

				writer.write(outStream.toString("UTF-8")) ;
			}

			reader.close();
		}

		writer.close();
	}

	private void finalizeLabels() throws IOException {
		
		//FileSystem fs = getFileSystem(workingDir) ;
		

		//merge two sets of labels
		//one from step 3, which includes all senses and link counts, but not term/doc counts.
		//one from step 4, which includes term/doc counts, but no senses or link counts.

		//both sets are ordered by label text, so this can be done in one pass with a merge operation.

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(finalDir).create(new Path(finalDir + "/label.csv")))) ;

		//gather label files from step 3	
		FileStatus[] labelFilesA = getFileSystem(workingDir).listStatus(new Path(workingDir + "/" + getDirectoryName(ExtractionStep.labelSense)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(LabelSensesStep.Output.tempLabel.name()) ;
			}
		}) ;

		//gather label files from step 4
		FileStatus[] labelFilesB = getFileSystem(workingDir).listStatus(new Path(workingDir + "/" + getDirectoryName(ExtractionStep.labelOccurrence)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(LabelSensesStep.Output.tempLabel.name()) ;
			}
		}) ;

		long bytesTotal = 0 ;

		for (FileStatus status:labelFilesA) 
			bytesTotal += status.getLen() ;
		for (FileStatus status:labelFilesA) 
			bytesTotal += status.getLen() ;

		ProgressTracker pt = new ProgressTracker(bytesTotal, "Finalizing labels", DumpExtractor.class) ;


		//Initialise file readers. 
		//Slightly hacky, but bytesRead and fileIndexes are single element arrays (rather than ints or longs) so they can be passed by reference.
		long[]  bytesRead = {0} ;

		int[] fileIndexA = {0} ;
		BufferedReader readerA = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(labelFilesA[fileIndexA[0]].getPath()))) ;

		int[] fileIndexB = {0} ;
		BufferedReader readerB = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(labelFilesB[fileIndexB[0]].getPath()))) ;

		String labelTextA = null ;
		String labelTextB = null ;

		ExLabel labelA = null ;
		ExLabel labelB = null ;

		while (true) {

			if (labelTextA == null && fileIndexA[0] < labelFilesA.length) {
				String line = getNextLine(readerA, labelFilesA, fileIndexA, bytesRead) ;
				if (line != null) {
					CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

					labelTextA = cri.readString("labelText") ;
					labelA = new ExLabel() ;
					labelA.deserialize(cri) ;
				}
			}

			if (labelTextB == null && fileIndexB[0] < labelFilesB.length) {
				String line = getNextLine(readerB, labelFilesB, fileIndexB, bytesRead) ;
				if (line != null) {
					CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

					labelTextB = cri.readString("labelText") ;
					labelB = new ExLabel() ;
					labelB.deserialize(cri) ;
				}
			}

			if (labelTextA == null && labelTextB == null) {
				//done
				break ;
			}

			if (labelTextA != null && labelTextB != null && labelTextA.equals(labelTextB)) {

				//merge these labels 
				labelA.setTextDocCount(labelB.getTextDocCount()) ;
				labelA.setTextOccCount(labelB.getTextOccCount()) ;

				if (labelA.getLinkOccCount() > labelA.getTextOccCount())
					Logger.getLogger(DumpExtractor.LOG_WEIRD_LABEL_COUNT).warn("Label '" + labelTextA + "' occurs " + labelA.getLinkOccCount() + " times as links, but only " + labelA.getTextOccCount() + " times in plain text.") ;

				if (labelA.getLinkDocCount() > labelA.getTextDocCount())
					Logger.getLogger(DumpExtractor.LOG_WEIRD_LABEL_COUNT).warn("Label '" + labelTextA + "' occurs in " + labelA.getLinkDocCount() + " documents as links, but only " + labelA.getTextDocCount() + " in plain text.") ;

				//print merged label
				ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

				CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
				cro.writeString(labelTextA, "labelText") ;
				convert(labelA).serialize(cro) ;

				writer.write(outStream.toString("UTF-8")) ;

				//advance both A and B
				labelA = null ;
				labelTextA = null ;

				labelB = null ;
				labelTextB = null ;

				continue ;
			}

			if (labelTextA != null && (labelTextB == null || labelTextA.compareTo(labelTextB) < 0)) {

				//found A but no corresponding B. This is OK if A is only a title or redirect, and never used as a link anchor. Otherwise it is worth warning about.
				if (labelA.getLinkOccCount() > 0)
					Logger.getLogger(DumpExtractor.LOG_WEIRD_LABEL_COUNT).warn("Found label '" + labelTextA + "' without any text occurances. It occurs in " + labelA.getLinkOccCount() + " links.") ;

				//write A
				ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

				CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
				cro.writeString(labelTextA, "labelText") ;
				convert(labelA).serialize(cro) ;

				writer.write(outStream.toString("UTF-8")) ;

				//advance A
				labelA = null ;
				labelTextA = null ;

			} else {

				//found B but no corresponding A. This shouldn't be possible. 
				Logger.getLogger(DumpExtractor.LOG_WEIRD_LABEL_COUNT).error("Found label '" + labelTextB + "' without any senses or link occurances.") ;

				//write B
				ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

				CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
				cro.writeString(labelTextB, "labelText") ;
				convert(labelB).serialize(cro) ;

				writer.write(outStream.toString("UTF-8")) ;

				//advance B
				labelB = null ;
				labelTextB = null ;

			}

			pt.update(bytesRead[0]) ;
		}

		writer.close();
	}

	private void finalizeStatistics(TreeMap<String, Long> stats) throws IOException {

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(workingDir).create(new Path(workingDir + "/" + OUTPUT_STATS)))) ;

		for(Map.Entry<String,Long> e:stats.entrySet()) {

			ByteArrayOutputStream outStream = new ByteArrayOutputStream() ;

			CsvRecordOutput cro = new CsvRecordOutput(outStream) ;
			cro.writeString(e.getKey(), null) ;
			cro.writeLong(e.getValue(), null) ;

			writer.write(outStream.toString("UTF-8")) ;
			writer.newLine() ;
		}

		writer.close();
	}

	private void finalizeFile(ExtractionStep step, final String filePrefix) throws IOException {
		
		
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getFileSystem(finalDir).create(new Path(finalDir + "/" + filePrefix + ".csv")))) ;

		FileStatus[] fileStatuses = getFileSystem(workingDir).listStatus(new Path(workingDir + "/" + getDirectoryName(step)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(filePrefix) ;
			}
		}) ;

		long bytesTotal = 0 ;
		for (FileStatus status:fileStatuses) {
			bytesTotal += status.getLen() ;
		}

		ProgressTracker pt = new ProgressTracker(bytesTotal, "finalizing " + filePrefix, DumpExtractor.class) ;
		long bytesRead = 0 ;

		for (FileStatus status:fileStatuses) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(getFileSystem(workingDir).open(status.getPath()))) ;

			String line = null;
			while ((line = reader.readLine()) != null) {
				bytesRead += line.length() + 1 ;
				pt.update(bytesRead) ;

				writer.write(line) ;
				writer.newLine() ;
			}

			reader.close() ;
		}

		writer.close();
	}

	public static DbLabel convert(ExLabel oldLabel) {

		ArrayList<DbSenseForLabel> senses = new ArrayList<DbSenseForLabel>() ;
		for (Map.Entry<Integer,ExSenseForLabel> entry:oldLabel.getSensesById().entrySet()) {

			DbSenseForLabel sense = new DbSenseForLabel() ;
			sense.setId(entry.getKey()) ;
			sense.setLinkOccCount(entry.getValue().getLinkOccCount()) ;
			sense.setLinkDocCount(entry.getValue().getLinkDocCount()) ;

			sense.setFromRedirect(entry.getValue().getFromRedirect()) ;
			sense.setFromTitle(entry.getValue().getFromTitle()) ;

			senses.add(sense) ;
		}

		Collections.sort(senses, new Comparator<DbSenseForLabel>() {

			public int compare(DbSenseForLabel a, DbSenseForLabel b) {

				int cmp = new Long(b.getLinkOccCount()).compareTo(a.getLinkOccCount()) ;
				if (cmp != 0)
					return cmp ;

				cmp = new Long(b.getLinkDocCount()).compareTo(a.getLinkDocCount()) ;
				if (cmp != 0)
					return cmp ;

				return(new Integer(a.getId()).compareTo(b.getId())) ;
			}
		}) ;


		DbLabel newLabel = new DbLabel() ; 

		newLabel.setLinkDocCount(oldLabel.getLinkDocCount()) ;
		newLabel.setLinkOccCount(oldLabel.getLinkOccCount()) ;
		newLabel.setTextDocCount(oldLabel.getTextDocCount()) ;
		newLabel.setTextOccCount(oldLabel.getTextOccCount()) ;

		newLabel.setSenses(senses) ;

		return newLabel ;
	}

	private String getNextLine(BufferedReader reader, FileStatus[] files, int[] fileIndex, long[] bytesRead) throws IOException {

		String line = reader.readLine() ;

		if (line==null) { 
			fileIndex[0]++ ;
			reader.close();

			if (fileIndex[0] < files.length) {
				Path path = files[fileIndex[0]].getPath() ;
				
				reader = new BufferedReader(new InputStreamReader(getFileSystem(path).open(path))) ;
				line = reader.readLine();

				bytesRead[0] = bytesRead[0] + line.length() + 1 ;
			}
		}

		return line ;		
	}

	private Long getLastEdit() throws IOException {
		
		FileSystem fs = getFileSystem(workingDir) ;
		

		FileStatus[] fileStatuses = fs.listStatus(new Path(workingDir + "/" + getDirectoryName(ExtractionStep.page)), new PathFilter() {
			public boolean accept(Path path) {				
				return path.getName().startsWith(PageStep.Output.tempEditDates.name()) ;
			}
		}) ;

		Long lastEdit = null ;

		for (FileStatus status:fileStatuses) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()))) ;

			String line = null;
			while ((line = reader.readLine()) != null) {

				CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

				int pageId = cri.readInt(null) ;
				long edit = cri.readLong(null) ;

				if (lastEdit == null || lastEdit < edit)
					lastEdit = edit ;
			}
		}

		return lastEdit ;

	}

	public static String getDirectoryName(ExtractionStep step) {
		StringBuffer s = new StringBuffer("temp") ;
		s.append(Character.toUpperCase(step.name().charAt(0))) ;
		s.append(step.name().substring(1)) ;

		return s.toString();
	}
}
