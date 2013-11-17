package org.wikipedia.miner.examples;

import java.io.File;

import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;

public class WikipediaDefiner {
	
	private static String confFile = "../configs/simplewiki.xml" ;

	 public static void main(String args[]) throws Exception {
			
	        WikipediaConfiguration conf = new WikipediaConfiguration(new File(confFile)) ;
				
	        Wikipedia wikipedia = new Wikipedia(conf, false) ;
		    
	        Article article = wikipedia.getArticleByTitle("Wikipedia") ;
		    
	        System.out.println(article.getSentenceMarkup(0)) ;
		    
	        wikipedia.close() ;
	    }
}
