package org.wikipedia.miner.examples;

import java.io.File;
import org.wikipedia.miner.comparison.ArticleComparer;

import org.wikipedia.miner.model.Page;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;

public class WikipediaDefiner {
	
	private static final String confFile = "/home/angel/wikiminer/configs/wikipedia-en.xml" ;

	 public static void main(String args[]) throws Exception {
			
	        WikipediaConfiguration conf = new WikipediaConfiguration(new File(confFile)) ;
                		conf.clearDatabasesToCache() ;

	        Wikipedia wikipedia = new Wikipedia(conf, false) ;
		    				ArticleComparer artCmp = new ArticleComparer(wikipedia) ;

	        Page article = wikipedia.getPageById(35096782);
		    
	        System.out.println(article.getSentenceMarkup(0)) ;
		    
	        wikipedia.close() ;
	    }
}
