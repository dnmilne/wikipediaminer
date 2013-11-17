/*
 *    SimpleDocumentIndexer.java
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

import org.wikipedia.miner.annotation.Topic;

/**
 * This class very simply weights topics according to how important they are within the document. 
 * <p>
 * This is done heuristically (without any training), by comparing how often the topics are mentioned, how spread out these mentions are in the document, and (most importantly) how strongly they relate to other topics that are mentioned.
 * <p>
 * Although the resulting weight will always be between 0 and 1, it is not a probability.
 * 
 * @author David Milne
 */
public class SimpleDocumentIndexer extends TopicWeighter{

	@Override
	public HashMap<Integer,Double> getTopicWeights(Collection<Topic> topics) throws Exception {
		
		HashMap<Integer, Double> topicWeights = new HashMap<Integer, Double>() ;
		
		
		int maxOccurances = 0 ;
		for (Topic topic:topics) {
			if (topic.getOccurances()>maxOccurances)
				maxOccurances = topic.getOccurances() ;
		}
		for (Topic topic:topics) {
			
			double weight = topic.getRelatednessToOtherTopics() * 2 ; 
			weight = weight + (float)topic.getOccurances()/maxOccurances ;
			weight = weight + topic.getSpread() ;
			weight = weight/3 ;
			
			topicWeights.put(topic.getId(), weight) ;
		}
		
		return topicWeights ;
	}
}
