package org.wikipedia.miner.extract.util;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.util.ProgressTracker;

public class RedirectCache {

	private static RedirectCache cache ;

	public static RedirectCache get() {

		if (cache == null)
			cache = new RedirectCache() ;

		return cache ;
	}


	
	
	private TIntIntHashMap redirectTargetsBySource ;
	private boolean isLoaded = false ;

	public RedirectCache() {
		redirectTargetsBySource = new TIntIntHashMap() ;
		
		
	}
	
	
	public boolean isLoaded() {
		return isLoaded ;
	}


	private long getBytes(List<Path> paths) {

		long bytes = 0 ;

		for (Path path:paths) {
			File file = new File(path.toString()) ;
			bytes = bytes + file.length() ;
		}

		return bytes ;
	}

	public void load(List<Path> redirectFiles, Reporter reporter) throws IOException {

		if (isLoaded)
			return ;
		
		Runtime r = Runtime.getRuntime() ;
		long memBefore = r.totalMemory() ;
		
		ProgressTracker tracker = new ProgressTracker(getBytes(redirectFiles), "Loading redirects", getClass()) ; 
		long bytesRead = 0 ;

		for (Path redirectFile:redirectFiles) {
			BufferedReader fis = new BufferedReader(new FileReader(redirectFile.toString()));
			String line = null;

			while ((line = fis.readLine()) != null) {

				bytesRead = bytesRead + line.length() + 1 ;
				tracker.update(bytesRead) ;

				try {
					String[] values = line.split(",") ;

					int sourceId = Integer.parseInt(values[0]) ;
					int targetId = Integer.parseInt(values[1]) ;

					redirectTargetsBySource.put(sourceId, targetId) ;	
					reporter.progress() ;
				} catch (Exception e) {
					Logger.getLogger(Util.class).error("Caught exception while gathering redirect from '" + line + "' in '" + redirectFile + "'", e);
				}
			}

			fis.close();
		}
		
		long memAfter = r.totalMemory() ;
		Logger.getLogger(getClass()).info("Memory Used: " + (memAfter - memBefore) / (1024*1024) + "Mb") ;
		
		isLoaded = true ;
	}



	public Integer getTargetId(int sourceId) {

		if (!redirectTargetsBySource.contains(sourceId))
			return null ;

		return redirectTargetsBySource.get(sourceId) ;

	}

	public Integer getTargetId(String targetTitle) throws IOException {

		Integer currId = PagesByTitleCache.getArticlesCache().getPageId(targetTitle) ;

		if (currId == null)
			return null ;

		TIntSet targetsSeen = new TIntHashSet() ;

		while (currId != null) {

			//if there is no entry for this id, then this isn't a redirect, so no need to continue
			if (!redirectTargetsBySource.containsKey(currId))
				return currId ;

			//otherwise we need to resolve the redirect
			if (targetsSeen.contains(currId)) {
				// seen this redirect before, so we have entered a loop
				return null ;
			} else {
				//recurse to the next id
				targetsSeen.add(currId) ;
				currId = redirectTargetsBySource.get(currId) ;
			}
		}

		return currId ;
	}


}
