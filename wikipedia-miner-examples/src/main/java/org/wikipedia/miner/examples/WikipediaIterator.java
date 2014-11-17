/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.wikipedia.miner.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Page;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.PageIterator;
import org.wikipedia.miner.util.WikipediaConfiguration;

/**
 *
 * @author angel
 */
public class WikipediaIterator {

    BufferedReader _input;
    Wikipedia _wikipedia;

    public WikipediaIterator(WikipediaConfiguration conf) {
        _input = new BufferedReader(new InputStreamReader(System.in));
        _wikipedia = new Wikipedia(conf, false);
    }

    public static void main(String args[]) throws Exception {
        WikipediaConfiguration conf = new WikipediaConfiguration(new File("/home/angel/wikiminer/configs/wikipedia-es.xml"));
        conf.clearDatabasesToCache();
        WikipediaIterator thesaurus = new WikipediaIterator(conf);
        thesaurus.iterate();

    }

    private void iterate() {
        PageIterator itr = _wikipedia.getPageIterator();
        int total = 0;
        int enTrans = 0;
        String prev="";
        String nex;
        StringBuffer redirectRegex = new StringBuffer("\\#") ;
		redirectRegex.append("(REDIRECT|REDIRECCIÓN") ;
		
		redirectRegex.append(")[:\\s]*(?:\\[\\[(.*)\\]\\]|(.*))") ;
                Pattern redirectPattern = Pattern.compile(redirectRegex.toString(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        while (itr.hasNext()) {
            Page next = itr.next();
            if (next.getType() == Page.PageType.article) {
//                System.out.println();
                //System.out.println("sadsad");
                Article arc = (Article) next;
                
                if(arc.getRedirects().length==0){
                    String mark=arc.getMarkup();
                    Matcher mat=(redirectPattern.matcher(mark));
                    

                if (!mat.find()) {
                    if (arc.getTranslations().containsKey("en")) {

                        System.out.println("trans" + enTrans++);
                    }
                  if(prev.equals(arc.getTitle())){
                      System.out.println(arc.getMarkup());
                      System.exit(1);
                                        }
                  prev=arc.getTitle();
//                for (Article.Label label : ((Article) next).getLabels()) {
//                    System.out.println(label.getText());
//
//                }
                    System.out.println(next.getTitle() + " " + (++total));
                }
                }
            }

        }
        itr.close();
        _wikipedia.close();
    }
}
