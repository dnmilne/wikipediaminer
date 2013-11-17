/*
 *    TopicWeighter.java
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

package org.wikipedia.miner.annotation.weighting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import org.wikipedia.miner.annotation.*;

/**
 * This abstract class specifies the methods that should be implemented by something that will weight topics identified by the topicDetector.
 * i.e. to identify those that are most central to the theme of the document, or those that are most worthy of being linked to.
 *  
 * @author David Milne
 */
public abstract class TopicWeighter {

	/**
	 * Weights and sorts the given topics according to some criteria. 
	 * 
	 * @param topics the topics to be weighted and sorted.
	 * @return the weighted topics.
	 * @throws Exception depends on the implementing class
	 */
	
	
	public abstract HashMap<Integer,Double> getTopicWeights(Collection<Topic> topics) throws Exception ;
	
	
	public ArrayList<Topic> getWeightedTopics(Collection<Topic> topics) throws Exception {
		
		HashMap<Integer,Double> topicWeights = getTopicWeights(topics) ;
		
		
		ArrayList<Topic> weightedTopics = new ArrayList<Topic>() ;

		for (Topic topic: topics) {
			
			if (topicWeights.containsKey(topic.getId()))
				topic.setWeight(topicWeights.get(topic.getId())) ;
			else
				topic.setWeight(0.0) ;
				
			weightedTopics.add(topic) ;
		}
		
		Collections.sort(weightedTopics) ;
		
		return weightedTopics ;
	}
	
	
	/**
	 * A convenience method that weights the given topics using getWeightedTopics(), and discards those below a certian weight. 
	 * 
	 * @param topics the topics to be weighted and sorted.
	 * @param minimumWeight the weight below which topics are discarded
	 * @return the weighted topics.
	 * @throws Exception depends on the implementing class
	 */
	public ArrayList<Topic> getBestTopics(Collection<Topic> topics, double minimumWeight) throws Exception {
		
		ArrayList<Topic> allTopics = getWeightedTopics(topics) ;
		ArrayList<Topic> bestTopics = new ArrayList<Topic>() ;
		
		for (Topic topic: allTopics) {
			if (topic.getWeight() >= minimumWeight) 
				bestTopics.add(topic) ;
			else
				break ;
		}
		
		return bestTopics ;		
	}
	
}
