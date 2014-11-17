/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.wikipedia.miner.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Category;
import org.wikipedia.miner.model.Label;
import org.wikipedia.miner.model.Page;
import org.wikipedia.miner.model.Redirect;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.PageIterator;
import org.wikipedia.miner.util.WikipediaConfiguration;

public class Wikisaurus {

    BufferedReader _input;
    Wikipedia _wikipedia;

    public Wikisaurus(WikipediaConfiguration conf) {
        _input = new BufferedReader(new InputStreamReader(System.in));
        _wikipedia = new Wikipedia(conf, false);
    }

    public static void main(String args[]) throws Exception {
        WikipediaConfiguration conf = new WikipediaConfiguration(new File("/home/angel/wikiminer/configs/wikipedia-en.xml"));
        conf.clearDatabasesToCache();
        Wikisaurus thesaurus = new Wikisaurus(conf);
//        thesaurus.iterate();
        thesaurus.run();
    }

    public void run() throws IOException, Exception {
        String term;
        while ((term = getString("Please enter a term to look up in Wikipedia")) != null) {

            Label label = _wikipedia.getLabel(term);
            Page pag=_wikipedia.getPageById(35241496);
            Redirect red= (Redirect)pag;
            if (!label.exists()) {
                System.out.println("I have no idea what '" + term + "' is");
            } else {
                Label.Sense[] senses = label.getSenses();
                if (senses.length == 0) {
                    displaySense(senses[0]);
                } else {
                    System.out.println("'" + term + "' could mean several things:");
                    for (int i = 0; i < senses.length; i++) {
                        System.out.println(" - [" + (i + 1) + "] " + senses[i].getTitle());
                    }
                    Integer senseIndex = getInt("So which do you want?", 1, senses.length);
                    if (senseIndex != null) {
                        displaySense(senses[senseIndex - 1]);
                    }
                }
            }
        }
    }

    private String getString(String prompt) throws IOException {

        System.out.println(prompt + "(or ENTER for none)");

        String line = _input.readLine();

        if (line.trim().equals("")) {
            line = null;
        }

        return line;
    }

    private Integer getInt(String prompt, int min, int max) throws IOException {

        while (true) {

            System.out.println(prompt + " (" + min + " - " + max + " or ENTER for none)");

            String line = _input.readLine();
            if (line.trim().equals("")) {
                return null;
            }

            try {
                Integer val = Integer.parseInt(line);
                if (val >= min && val <= max) {
                    return val;
                }
            } catch (Exception e) {
            }

            System.out.println("Invalid input, try again");
        }

    }

    protected void displaySense(Label.Sense sense) throws Exception {

        System.out.println("==" + sense.getTitle() + "==");

        displayDefinition(sense);
        displayAlternativeLabels(sense);
        displayRelatedTopics(sense);
       
    }

    protected void displayDefinition(Label.Sense sense) throws Exception {
        System.out.println(sense.getSentenceMarkup(0));
        System.out.println(sense.getMarkup().substring(0, 5000));
        System.out.println("\n______Parent Cateories__________\n");
        for (Category object : sense.getParentCategories()) {
            System.out.println("-"+object.getTitle());
        }
        System.out.println("\n______InfoBox__________\n");
        if(sense.getType()==Page.PageType.article){
//            System.out.println(((Article)sense).getInfoBoxTitle());
        }else{
            System.out.println("not an article");
        }
    }

    protected void displayAlternativeLabels(Label.Sense sense) throws Exception {
        System.out.println("\nAlternative labels:");
        for (Article.Label label : sense.getLabels()) {
            System.out.println(label.getText());

        }
    }

    protected void displayRelatedTopics(Label.Sense sense) throws Exception {
        System.out.println("\nRelated topics:");
        for (Article art : sense.getLinksOut()) {
            System.out.println(" - " + art.getTitle());
        }
    }
    
  


    private void iterate() {
        PageIterator itr = _wikipedia.getPageIterator();
        int i = 0;
        while (itr.hasNext()) {
            Page next = itr.next();
            System.out.println(next.getTitle());
            if (next.getType() == Page.PageType.article) {
                System.out.println("sadsad");
                Article arc= (Article) next;
                System.out.println(arc.getMarkup());
//                for (Article.Label label : ((Article) next).getLabels()) {
//                    System.out.println(label.getText());
//
//                }

            }
            System.out.println(( ++i));
        }
        itr.close();
        _wikipedia.close();
    }
}
