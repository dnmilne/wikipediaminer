

package org.wikipedia.miner.examples;

import java.io.File;
import java.io.IOException;
import java.text.Normalizer;
import java.util.regex.Pattern;
import org.wikipedia.miner.db.WEnvironment;
import org.wikipedia.miner.util.WikipediaConfiguration;
import org.wikipedia.miner.util.text.CaseFolder;
import org.wikipedia.miner.util.text.english.CaseAccentSimpleTextProcessor;
import org.wikipedia.miner.util.text.english.PorterStemmer;
import org.wikipedia.miner.util.text.english.SimpleStemmer;

/**
 *
 * @author angel
 */
public class PrepareTextProcessor {
   private final CaseFolder caseFolder=new CaseFolder();
    private final SimpleStemmer stemmer= new SimpleStemmer();
    private final Pattern pattern =Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    
        public String processText(String string) {
        String normalizedText = Normalizer.normalize(string, Normalizer.Form.NFD);
        normalizedText=pattern.matcher(normalizedText).replaceAll("");
        normalizedText=caseFolder.processText(normalizedText);
        normalizedText=stemmer.processText(normalizedText);
        return normalizedText;
    }
    public static void main(String[] args) throws IOException,Exception {
        PorterStemmer tex=new PorterStemmer();
        WikipediaConfiguration conf=new WikipediaConfiguration(new File("/home/angel/wikiminer/configs/wikipedia-en.xml"));
        WEnvironment.prepareTextProcessor(tex, conf, new File("/tmp/"), true, 4);
             }   
    }
