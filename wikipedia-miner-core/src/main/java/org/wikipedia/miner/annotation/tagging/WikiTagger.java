/*
 *    WikiTagger.java
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

package org.wikipedia.miner.annotation.tagging;

import org.wikipedia.miner.annotation.Topic;

/**
 * This tagger will replace topic mentions with the appropriate mediawiki markup.
 * 
 * e.g. "what a cute puppy" would become "[[Dog|what a cute puppy]]"
 * 
 * @author David Milne
 */
public class WikiTagger extends DocumentTagger{
		
	@Override
	public String getTag(String anchor, Topic topic) {
		if (topic.getTitle().compareToIgnoreCase(anchor) == 0)
			return "[[" + anchor + "]]" ;
		else
			return "[[" + topic.getTitle() + "|" + anchor + "]]" ;
	}
}
