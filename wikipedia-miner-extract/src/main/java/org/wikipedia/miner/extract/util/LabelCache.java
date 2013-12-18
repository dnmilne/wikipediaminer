package org.wikipedia.miner.extract.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.wikipedia.miner.util.ProgressTracker;

import gnu.trove.set.hash.THashSet;

public class LabelCache {
	
	private static Logger logger = Logger.getLogger(LabelCache.class) ;

	private static LabelCache cache ;

	public static LabelCache get() throws IOException {
		if (cache == null)
			cache = new LabelCache() ;

		return cache ;
	}

	Set<String> labelVocabulary ;


	public LabelCache() throws IOException {
		
		DB db = DBMaker.newAppendFileDB(File.createTempFile("mapdb-temp", "labels"))
	       .deleteFilesAfterClose().closeOnJvmShutdown().transactionDisable().cacheHardRefEnable().make();

		
		labelVocabulary = db.getHashSet("labels") ;
	}
	
	
	private boolean isLoaded = false ;

	public boolean isLoaded() {
		return isLoaded ;
	}

	public boolean isKnown(String label) {
		return labelVocabulary.contains(label) ;
	}

	private long getBytes(List<Path> paths) {

		long bytes = 0 ;

		for (Path path:paths) {
			File file = new File(path.toString()) ;
			bytes = bytes + file.length() ;
		}

		return bytes ;
	}

	public void load(List<Path> paths, Reporter reporter) throws IOException {

		if (isLoaded)
			return ;
		
		Runtime r = Runtime.getRuntime() ;
		long memBefore = r.totalMemory() ;
		
		ProgressTracker tracker = new ProgressTracker(getBytes(paths), "Loading labels", getClass()) ;

		long bytesRead = 0 ;

		for (Path path:paths) {

			BufferedReader fis = new BufferedReader(new FileReader(path.toString()));
			String line = null;

			while ((line = fis.readLine()) != null) {

				bytesRead = bytesRead + line.length() + 1 ;
				tracker.update(bytesRead) ;

				try {

					CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream(line.getBytes("UTF8"))) ;
					String labelText = cri.readString("labelText") ;
					labelVocabulary.add(labelText) ;


				} catch (Exception e) {
					logger.error("Caught exception while gathering label from '" + line + "' in '" + path + "'", e);
				}

				reporter.progress() ;

			}

			fis.close() ;
		}
		
		long memAfter = r.totalMemory() ;
		logger.info("Memory Used: " + (memAfter - memBefore) / (1024*1024) + "Mb") ;
		
		isLoaded = true ;
		
		
		
	}


}
