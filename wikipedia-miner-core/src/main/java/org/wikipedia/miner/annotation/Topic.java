/*
 *    Topic.java
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

import java.util.Vector;

import org.wikipedia.miner.model.*;
import org.wikipedia.miner.util.*;

/**
 * This class represents a topic that was automatically detected and disambiguated in a document.
 * 
 * @author David Milne
 */
public class Topic extends Article{

	Vector<Position> positions ;

	private double relatednessToContext ;
	private double relatednessToAllTopics ;
	private double totalLinkProbability ;
	private double maxLinkProbability ;

	private double totalDisambigConfidence ;
	private double maxDisambigConfidence ;
	private double docLength ;

	/**
	 * Initializes a new topic 
	 * 
	 * @param wikipedia an active instance of Wikipedia
	 * @param id the id of the article that this topic represents
	 * @param relatednessToContext the extent to which this topic relates to the surrounding unambiguous context
	 * @param docLength the length of the document, in characters
	 */
	public Topic(Wikipedia wikipedia, int id, double relatednessToContext, double docLength) {
		super(wikipedia.getEnvironment(), id) ;

		this.relatednessToContext = relatednessToContext ;
		this.relatednessToAllTopics = -1 ;
		this.docLength = docLength ;
		
		positions = new Vector<Position>() ;
		totalLinkProbability = 0 ;
		maxLinkProbability = 0 ;
		totalDisambigConfidence = 0 ;
		maxDisambigConfidence = 0 ;
	}

	/**
	 * Adds an ngram occurance in the document that refers to this topic 
	 * 
	 * @param reference	the refering ngram (and it's location)
	 * @param disambigConfidence the confidence with which the disambiguator chose this topic as the correct sense for the ngram
	 */
	public void addReference(TopicReference reference, double disambigConfidence){
		positions.add(reference.getPosition()) ;

		double prob = reference.getLabel().getLinkProbability() ;

		totalLinkProbability = totalLinkProbability + prob ;
		if (prob > maxLinkProbability)
			maxLinkProbability = prob ;
		

		totalDisambigConfidence = totalDisambigConfidence + disambigConfidence ;
		if (disambigConfidence > maxDisambigConfidence)
			maxDisambigConfidence = disambigConfidence ; 
	}
	
	/**
	 * @return the locations in this document that refer to this topic
	 */
	public Vector<Position> getPositions() {
		return positions ;
	}

	/**
	 * @return the number of times this topic is refered to.
	 */
	public int getOccurances() {
		return positions.size() ;
	}
	
	public double getNormalizedOccurances() {
		return Math.log(positions.size() + 1) ;
	}

	/**
	 * @return the extent to which this topic relates to surrounding unambiguous context.
	 */
	public double getRelatednessToContext() {
		return relatednessToContext ;
	}
	
	/**
	 * @return the extent to which this topic relates to all other topics detected in the document.
	 * @throws Exception if this has not been calculated yet (this is the last step performed by the topic detector).
	 */
	public double getRelatednessToOtherTopics() throws Exception{
		
		if (relatednessToAllTopics < 0) {
			throw new Exception("Relatedness to context not calcuated yet!") ;
		}
		
		return relatednessToAllTopics ;
	}
	
	/**
	 * Sets the relatedness of this topic to all other topics detected in the document. 
	 * 
	 * @param r the extent to which this topic relates to all other topics detected in the document.
	 */
	protected void setRelatednessToOtherTopics(float r) {
		this.relatednessToAllTopics = r ;
	}

	/** 
	 * @return the maximum probability that the ngrams which refer to this topic would be links (rather than plain text) if found in a random wikipedia article.
	 */
	public double getMaxLinkProbability() {
		return maxLinkProbability ;
	}
	
	public double getNormalizedMaxLinkProbability() {
		double mlp = getMaxLinkProbability() ;
		
		mlp = Math.log((mlp*1000) + 1) ;
		mlp = mlp/4 ;
		
		return mlp ;
	}

	/** 
	 * @return the average probability that the ngrams which refer to this topic would be links (rather than plain text) if found in a random wikipedia article.
	 */
	public double getAverageLinkProbability() {
		return totalLinkProbability/positions.size() ;
	}
	
	public double getNormalizedAverageLinkProbability() {
		double alp = getAverageLinkProbability() ;
		
		alp = Math.log((alp*1000) + 1) ;
		alp = alp/4 ;
		
		return alp ;
	}

	/** 
	 * @return the maximum confidence with which the disambiguator chose this topic as the correct sense for the ngrams from which it was mined.
	 */
	public double getMaxDisambigConfidence() {
		return maxDisambigConfidence ;
	}

	/** 
	 * @return the average confidence with which the disambiguator chose this topic as the correct sense for the ngrams from which it was mined.
	 */
	public double getAverageDisambigConfidence() {
		return totalDisambigConfidence/positions.size() ;
	}

	/**
	 * @return the distance between the start of the document and the first occurance of this topic, normalized by document length
	 */
	public double getFirstOccurance() {
		Position start = positions.firstElement() ;
		return ((double)start.getStart()) / docLength ;
	}

	/**
	 * @return the distance between the end of the document and the last occurance of this topic, normalized by document length
	 */
	public double getLastOccurance() {
		Position end = positions.lastElement() ;
		return ((double)end.getStart()) / docLength ;			
	}

	/**
	 * @return the distance between the first and last occurances of this topic, normalized by document length
	 */
	public double getSpread() {
		return getLastOccurance() - getFirstOccurance() ;
	}
}
