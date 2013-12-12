/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.wikipedia.miner.util.text;

import java.io.File;
import java.io.IOException;
import java.text.Normalizer;
import java.util.regex.Pattern;
import org.wikipedia.miner.db.WEnvironment;
import org.wikipedia.miner.util.WikipediaConfiguration;

/**
 * Class that normalizes,processes case folding and processes plurals using 
 * PlingStemmer from http://mpii.de/yago-naga/javatools 
 * @author angel
 */
    public class CaseAccentSimpleTextProcessor  extends  TextProcessor   {
  
    private final CaseFolder caseFolder=new CaseFolder();
    private final PlingStemmer stemmer= new PlingStemmer();
    private final Pattern pattern =Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    
    @Override
    public String processText(String string) {
        String normalizedText = Normalizer.normalize(string, Normalizer.Form.NFD);
        normalizedText=pattern.matcher(normalizedText).replaceAll("");
        normalizedText=caseFolder.processText(normalizedText);
        normalizedText=stemmer.stem(normalizedText);
        return normalizedText;
    }
//    public static void main(String[] args) throws IOException,Exception {
//        CaseAccentSimpleTextProcessor tex=new CaseAccentSimpleTextProcessor();
//        WikipediaConfiguration conf=new WikipediaConfiguration(new File("/home/angel/wikiminer/configs/wikipedia-en.xml"));
//        WEnvironment.prepareTextProcessor(tex, conf, new File("/tmp/"), true, 4);
//         
//    }   
    }
        