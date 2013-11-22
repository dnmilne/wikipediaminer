package org.wikipedia.miner.extract.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.log4j.Logger;
import org.wikipedia.miner.db.struct.DbPage;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.util.ProgressTracker;

import gnu.trove.map.hash.TObjectIntHashMap;

public class PagesByTitleCache {

	//this is a little dodgy, but if we reuse a jvm instance, we don't want to go to the (slow) process of reloading the page cache again. So we keep it as a singleton outside of mapper
	
	private static PagesByTitleCache articlesCache ;
	private static PagesByTitleCache categoriesCache ;
	
	private TObjectIntHashMap<String> pageIdsByTitle = new TObjectIntHashMap<String>() ;
	private Set<PageType> acceptablePageTypes = new HashSet<PageType>() ;
	
	
	private boolean isLoaded = false ;
	
	public static PagesByTitleCache getArticlesCache() {
		
		if (articlesCache == null) {
			articlesCache = new PagesByTitleCache() ;
			
			articlesCache.acceptablePageTypes.add(PageType.article) ;
			articlesCache.acceptablePageTypes.add(PageType.redirect) ;
			articlesCache.acceptablePageTypes.add(PageType.disambiguation) ;
		}
		
		return articlesCache ;
	}
	
	public static PagesByTitleCache getCategoriesCache() {
		
		if (categoriesCache == null) {
			categoriesCache = new PagesByTitleCache() ;
			
			categoriesCache.acceptablePageTypes.add(PageType.category) ;
		}
		
		return categoriesCache ;
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
	
	public void load(List<Path> pageFiles, Reporter reporter) throws IOException {
	
		if (isLoaded)
			return ;
		
		Runtime r = Runtime.getRuntime() ;
		long memBefore = r.totalMemory() ;
		
		ProgressTracker tracker = new ProgressTracker(getBytes(pageFiles), "Loading page files", getClass()) ;
		
		long bytesRead = 0 ;
		
		for (Path pageFile:pageFiles) {
			
			BufferedReader fis = new BufferedReader(new FileReader(pageFile.toString()));
			String line = null;
		
			while ((line = fis.readLine()) != null) {
				bytesRead = bytesRead + line.length() + 1 ;
				tracker.update(bytesRead) ;
				
				try {
					CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((line + "\n").getBytes("UTF-8"))) ;
	
					int id = cri.readInt("id") ;
					DbPage page = new DbPage() ;
					page.deserialize(cri) ;
	
					String title = page.getTitle() ;
					PageType type = PageType.values()[page.getType()] ;
					
					if (!acceptablePageTypes.contains(type))
						continue ;
					
					pageIdsByTitle.put(Util.normaliseTitle(title), id) ;
					reporter.progress() ;
					
				} catch (Exception e) {
					Logger.getLogger(Util.class).error("Caught exception while gathering page from '" + line + "' in '" + pageFile + "'", e) ;
				}
			}
			
			fis.close() ;
		}
		
		long memAfter = r.totalMemory() ;
		Logger.getLogger(getClass()).info("Memory Used: " + (memAfter - memBefore) / (1024*1024) + "Mb") ;
		
		isLoaded = true ;
	}
	
	public Integer getPageId(String title) {
		
		String nTitle = Util.normaliseTitle(title) ;
		
		if (!pageIdsByTitle.contains(nTitle))
			return null ;
		
		return pageIdsByTitle.get(nTitle) ;
	}

	
}
