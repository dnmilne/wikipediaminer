/*
 *    TopicDetector.java
 *    Copyright (C) 2007 David Milne, d.n.milne@gmail.com
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */


package org.wikipedia.miner.annotation;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.util.Span;

import org.wikipedia.miner.model.*;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.util.*;
import org.wikipedia.miner.util.NGrammer.NGramSpan;
import org.wikipedia.miner.annotation.preprocessing.*;

/**
 * This class detects topics that occur in plain text, using Disambiguator to resolve ambiguous terms and phrases. 
 * Many of the detected topics will be rubbish (extracted from unhelpful terms, such as <em>and</em> or <em>the</em>, so you will probably want to use either a LinkDetector or 
 * some simple heuristics to weed out the least useful ones (see Topic for the features that are available for separating important topics from less helpful ones). 
 * <p>
 * This also doesn't resolve collisions (e.g. "united states" collides with "states of america" in "united states of america"). 
 * The DocumentTagger has methods to resolve these.
 * 
 *  @author David Milne 
 */
public class TopicDetector {
	
	public enum DisambiguationPolicy {STRICT, LOOSE} ;
	
	private Wikipedia wikipedia ;
	private Disambiguator disambiguator ;
	
	
	private DisambiguationPolicy disambigPolicy = DisambiguationPolicy.STRICT;
	private boolean allowDisambiguations = false ;
	
	private int maxTopicsForRelatedness = 25 ;
	
	private NGrammer nGrammer ;
	
	
	/**
	 * Initializes a new topic detector.
	 * 
	 * @param wikipedia an initialized instance of Wikipedia
	 * @param disambiguator a trained 
	 * @param stopwordFile an optional (may be null) file containing 
	 * @param strictDisambiguation
	 * @param allowDisambiguations 
	 * @throws IOException 
	 */
	public TopicDetector(Wikipedia wikipedia, Disambiguator disambiguator) throws IOException {
		this.wikipedia = wikipedia ;
		this.disambiguator = disambiguator ;
		
		this.nGrammer = new NGrammer(wikipedia.getConfig().getSentenceDetector(), wikipedia.getConfig().getTokenizer()) ;
		this.nGrammer.setMaxN(disambiguator.getMaxLabelLength()) ;
		
		//TODO:Check caching 
		/*
		if (!wikipedia.getEnvironment().isGeneralityCached()) 
			System.err.println("TopicDetector | Warning: generality has not been cached, so this will run significantly slower than it needs to.") ;
		*/	
		
		
		
	}
	
	public DisambiguationPolicy getDisambiguationPolicy() {
		return disambigPolicy ;
	}
	
	public void setDisambiguationPolicy(DisambiguationPolicy dp) {
		disambigPolicy = dp ;
	}
	
	public boolean areDisambiguationsAllowed() {
		return allowDisambiguations ;
	}
	
	public void allowDisambiguations(boolean val) {
		allowDisambiguations = val ;
	}
	
	/**
	 * Gathers a collection of topics from the given document. 
	 * 
	 * @param doc a document that has been preprocessed so that markup (html, mediawiki, etc) is safely ignored.
	 * @param rc a cache in which relatedness measures will be saved so they aren't repeatedly calculated. This may be null.  
	 * @return a vector of topics that were mined from the document.
	 * @throws Exception
	 */
	public Vector<Topic> getTopics(PreprocessedDocument doc, RelatednessCache rc) throws Exception {
		
		if (rc == null)
			rc = new RelatednessCache(disambiguator.getArticleComparer()) ;
		

		//Vector<String> sentences = ss.getSentences(doc.getPreprocessedText(), SentenceSplitter.MULTIPLE_NEWLINES) ;
		Vector<TopicReference> references = getReferences(doc.getPreprocessedText()) ;
		
		Collection<Topic> temp = getTopics(references, doc.getContextText(), doc.getOriginalText().length(), rc).values() ;
		calculateRelatedness(temp, rc) ;

		Vector<Topic> topics = new Vector<Topic>() ;
		for (Topic t:temp) {
			if (!doc.isTopicBanned(t.getId())) 
					topics.add(t) ;
		}
		
		return topics ;
	}
	
	/**
	 * Gathers a collection of topics from the given document. 
	 * 
	 * @param text text to mine topics from. This must be plain text, without any form of markup. 
	 * @param rc a cache in which relatedness measures will be saved so they aren't repeatedly calculated. This may be null. 
	 * @return a collection of topics that were mined from the document.
	 * @throws Exception
	 */
	public Collection<Topic> getTopics(String text, RelatednessCache rc) throws Exception {
		
		if (rc == null)
			rc = new RelatednessCache(disambiguator.getArticleComparer()) ;
			

		//Vector<String> sentences = ss.getSentences(text, SentenceSplitter.MULTIPLE_NEWLINES) ;
		Vector<TopicReference> references = getReferences(text) ;
		
		HashMap<Integer,Topic> topicsById = getTopics(references, "", text.length(), rc) ;

		Collection<Topic> topics = topicsById.values() ;
		calculateRelatedness(topics, rc) ;
		
		return topics ;
	}
	
	private void calculateRelatedness(Collection<Topic> topics, RelatednessCache cache) throws Exception{
		
		TreeSet<Article> weightedTopics = new TreeSet<Article>() ;
		
		for (Topic t:topics) {
			if (t.getType() != PageType.article)
				continue ;
			
			Article art = (Article)wikipedia.getPageById(t.getId()) ;
			
			art.setWeight(t.getAverageLinkProbability() * t.getOccurances()) ;
			weightedTopics.add(art) ;
		}
		
		for (Topic topic: topics) {
			
			double totalWeight = 0 ;
			double totalWeightedRelatedness = 0 ;
			
			int count = 0 ;
			
			for (Article art: weightedTopics) {
				if (count++ > maxTopicsForRelatedness)
					break ;
				
				double weightedRelatedness = art.getWeight() * cache.getRelatedness(topic, art) ;
				
				totalWeight = totalWeight + art.getWeight();
				totalWeightedRelatedness = totalWeightedRelatedness + weightedRelatedness;
				
			}
			
			topic.setRelatednessToOtherTopics((float)(totalWeightedRelatedness/totalWeight)) ;
		}
	}
	
	
	
	
	private Vector<TopicReference> getReferences(String text) {
		
		Vector<TopicReference> references = new Vector<TopicReference>() ;
		for (NGramSpan span:nGrammer.ngramPosDetect(text)) {
						
			Label label = wikipedia.getLabel(span, text) ;
			
			//System.out.println(" - " + label.getText() + ", " + label.exists() + ", " + label.getLinkProbability() + "," + label.getLinkDocCount()) ;
			
			if (!label.exists())
				continue ;
			
			if (label.getLinkProbability() < disambiguator.getMinLinkProbability())
				continue ;
			
			//if (label.getLinkDocCount() < wikipedia.getConfig().getMinLinksIn())
			//	continue ;
			
			
			//System.out.println("adding ref: " + label.getText()) ;
			TopicReference ref = new TopicReference(label, new Position(span.getStart(), span.getEnd())) ;
			references.add(ref) ;
		}
		return references ;
	}
	
	private HashMap<Integer,Topic> getTopics(Vector<TopicReference> references, String contextText, int docLength, RelatednessCache cache) throws Exception{
		HashMap<Integer,Topic> chosenTopics = new HashMap<Integer,Topic>() ;
	
		/*
		// get context articles from unambiguous Labels
		Vector<Label> unambigLabels = new Vector<Label>() ;
		for (TopicReference ref:references) {
			Label label = ref.getLabel() ;
			
			Label.Sense[] senses = label.getSenses() ;
			if (senses.length > 0) {				
				if (senses.length == 1 || senses[0].getPriorProbability() > 1-disambiguator.getMinSenseProbability())
					unambigLabels.add(label) ;	
			}		
		}
		
		//get context articles from additional context text
		for (TopicReference ref:getReferences(contextText)){
			Label label = ref.getLabel() ;
			Label.Sense[] senses = label.getSenses() ;
			if (senses.length > 0) {
				if (senses.length == 1 || senses[0].getPriorProbability() > 1-disambiguator.getMinSenseProbability()) {
					unambigLabels.add(label) ;	
				}
			}
		}
		*/
		
		HashSet<String> detectedLabels = new HashSet<String>() ;
		Vector<Label> labels = new Vector<Label>() ;
		for (TopicReference ref:references) {
			if (detectedLabels.contains(ref.getLabel().getText())) 
				continue ;
			
			labels.add(ref.getLabel()) ;
			detectedLabels.add(ref.getLabel().getText()) ;		
		}
		
		//get context articles from additional context text
		for (TopicReference ref:getReferences(contextText)){
			if (detectedLabels.contains(ref.getLabel().getText())) 
				continue ;
			
			labels.add(ref.getLabel()) ;
			detectedLabels.add(ref.getLabel().getText()) ;	
		}
		
		
		
		
		Context context ;
		if (cache == null)
			context = new Context(labels, new RelatednessCache(disambiguator.getArticleComparer()), disambiguator.getMaxContextSize(), disambiguator.getMinSenseProbability() * 5) ;
		else 
			context = new Context(labels, cache, disambiguator.getMaxContextSize(), disambiguator.getMinSenseProbability()) ;	
		
		labels = null ;

		//now disambiguate all references
		//unambig references are still processed here, because we need to calculate relatedness to context anyway.
		
		// build a cache of valid senses for each phrase, since the same phrase may occur more than once, but will always be disambiguated the same way
		HashMap<String, ArrayList<CachedSense>> disambigCache = new HashMap<String, ArrayList<CachedSense>>() ;

		for (TopicReference ref:references) {
			//System.out.println("disambiguating ref: " + ref.getLabel().getText()) ;

			ArrayList<CachedSense> validSenses = disambigCache.get(ref.getLabel().getText()) ;

			if (validSenses == null) {
				// we havent seen this label in this document before
				validSenses = new ArrayList<CachedSense>() ;

				for (Label.Sense sense: ref.getLabel().getSenses()) {
					
					if (sense.getPriorProbability() < disambiguator.getMinSenseProbability()) break ;
					
					if (!allowDisambiguations && sense.getType() == PageType.disambiguation)
						continue ;

					double relatedness = context.getRelatednessTo(sense) ;
					double commonness = sense.getPriorProbability() ;

					double disambigProb = disambiguator.getProbabilityOfSense(commonness, relatedness, context) ;

					//System.out.println(" - sense " + sense + ", " + disambigProb) ;
					
					if (disambigProb > 0.1) {
						// there is at least a chance that this is a valid sense for the link (there may be more than one)
						
						CachedSense vs = new CachedSense(sense.getId(), commonness, relatedness, disambigProb) ;
						validSenses.add(vs) ;
					}
				}
				Collections.sort(validSenses) ;
				
				
				disambigCache.put(ref.getLabel().getText(), validSenses) ;
			}

			if (disambigPolicy == DisambiguationPolicy.STRICT) {
				//just get top sense
				if (!validSenses.isEmpty()) {
					CachedSense sense = validSenses.get(0) ;
					Topic topic = chosenTopics.get(sense.id) ;
	
					if (topic == null) {
						// we havent seen this topic before
						topic = new Topic(wikipedia, sense.id, sense.relatedness, docLength) ;
						chosenTopics.put(sense.id, topic) ;
					}
					topic.addReference(ref, sense.disambigConfidence) ;
				}
			} else {
				//get all senses
				for (CachedSense sense: validSenses) {
					Topic topic = chosenTopics.get(sense.id) ;

					if (topic == null) {
						// we haven't seen this topic before
						topic = new Topic(wikipedia, sense.id, sense.relatedness, docLength) ;
						chosenTopics.put(sense.id, topic) ;
					}
					topic.addReference(ref, sense.disambigConfidence) ;
				}
			}
		}
		
		
		return chosenTopics ;
	}
	

	
	

	private class CachedSense implements Comparable<CachedSense>{
		
		int id ;
		double commonness ;
		double relatedness ;
		double disambigConfidence ;

		/**
		 * Initializes a new CachedSense
		 * 
		 * @param id the id of the article that represents this sense
		 * @param commonness the prior probability of this sense given a source ngram (label)
		 * @param relatedness the relatedness of this sense to the surrounding unambiguous topics
		 * @param disambigConfidence the probability that this sense is valid, as defined by the disambiguator.
		 */
		public CachedSense(int id, double commonness, double relatedness, double disambigConfidence) {
			this.id = id ;
			this.commonness = commonness ;
			this.relatedness = relatedness ;
			this.disambigConfidence = disambigConfidence ;			
		}
		
		public int compareTo(CachedSense sense) {
			return -1 * Double.valueOf(disambigConfidence).compareTo(Double.valueOf(sense.disambigConfidence)) ;
		}
	}
}
