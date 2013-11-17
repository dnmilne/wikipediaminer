package org.wikipedia.miner.extract.util;


import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.*;
import java.util.* ;


import org.apache.hadoop.fs.Path;

import org.wikipedia.miner.db.struct.* ;
import org.wikipedia.miner.model.Page.PageType;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.log4j.Logger;

public class Util {


	public static String normaliseTitle(String title) {

		StringBuffer s = new StringBuffer() ;

		s.append(Character.toUpperCase(title.charAt(0))) ;
		s.append(title.substring(1).replace('_', ' ')) ;

		return s.toString() ;
	}

	public static TObjectIntHashMap<String> gatherPageIdsByTitle(Path pageFile, HashSet<PageType> acceptableTypes, TObjectIntHashMap<String> pagesByTitle, Reporter reporter) throws IOException {

		BufferedReader fis = new BufferedReader(new FileReader(pageFile.toString()));
		String line = null;

		while ((line = fis.readLine()) != null) {
			try {
				CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;

				int id = cri.readInt("id") ;
				DbPage page = new DbPage() ;
				page.deserialize(cri) ;

				String title = page.getTitle() ;
				PageType type = PageType.values()[page.getType()] ;

				if (acceptableTypes.contains(type))
					pagesByTitle.put(normaliseTitle(title), id) ;
				
				reporter.progress() ;
				
			} catch (Exception e) {
				Logger.getLogger(Util.class).error("Caught exception while gathering page from '" + line + "' in '" + pageFile + "'", e) ;
			}
		}
		
		return pagesByTitle ;
	}

	public static TIntIntHashMap gatherRedirectTargetsBySource(Path redirectFile, TIntIntHashMap redirectTargetsBySource, Reporter reporter) throws IOException {

		BufferedReader fis = new BufferedReader(new FileReader(redirectFile.toString()));
		String line = null;

		while ((line = fis.readLine()) != null) {
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
		
		return redirectTargetsBySource ;
	}
	
	public static Integer getTargetId(String targetTitle, TObjectIntHashMap<String> pagesByTitle, TIntIntHashMap redirectTargetsBySource) {
		
		String nTitle = normaliseTitle(targetTitle) ;
		
		if (!pagesByTitle.containsKey(nTitle))
			return null ;
		
		Integer target = pagesByTitle.get(nTitle) ;	
		HashSet<Integer> targetsSeen = new HashSet<Integer>() ;
		
	    while (target != null && redirectTargetsBySource != null && redirectTargetsBySource.containsKey(target)){
			
	    	if (targetsSeen.contains(target)) {
	    		// seen this redirect before, and can't resolve a loop
	    		return null ;
	    	} else {
	    		targetsSeen.add(target) ;
	    		
	    		if (redirectTargetsBySource.containsKey(target)) 
	    			target = redirectTargetsBySource.get(target) ;
	    		else
	    			target = null ;
			}
	    }
	    
		return target ;
	}

}
