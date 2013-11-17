/*
 *    PageIterator.java
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

package org.wikipedia.miner.util;

import java.util.* ;


import org.wikipedia.miner.db.WEntry;
import org.wikipedia.miner.db.WIterator;
import org.wikipedia.miner.db.WEnvironment;
import org.wikipedia.miner.db.struct.DbPage;
import org.wikipedia.miner.model.* ;
import org.wikipedia.miner.model.Page.PageType;


/**
 * @author David Milne
 * 
 * Provides efficient iteration over the pages in Wikipedia
 */
public class PageIterator implements Iterator<Page> {

	WEnvironment env ;
	WIterator<Integer,DbPage> iter ;

	Page nextPage = null ;
	PageType type = null ;

	/**
	 * Creates an iterator that will loop through all pages in Wikipedia.
	 * 
	 * @param database an active (connected) Wikipedia database.
	 */
	public PageIterator(WEnvironment env) {

		this.env = env ;
		iter = env.getDbPage().getIterator() ; 
		
		queueNext() ;
	}

	/**
	 * Creates an iterator that will loop through all pages of the given type in Wikipedia.
	 * 
	 * @param database an active (connected) Wikipedia database.
	 * @param pageType the type of page to restrict the iterator to (ARTICLE, CATEGORY, REDIRECT or DISAMBIGUATION_PAGE)
	 * @throws SQLException if there is a problem with the Wikipedia database.
	 */
	public PageIterator(WEnvironment env, PageType type)  {

		this.env = env ;
		iter = env.getDbPage().getIterator() ; 
		this.type = type ;
		
		queueNext() ;
	}

	public boolean hasNext() {
		return (nextPage != null) ;
	}

	public void remove() {
		throw new UnsupportedOperationException() ;
	}

	public Page next() {
		
		if (nextPage == null) 
			throw new NoSuchElementException() ;
		
		Page p = nextPage ;
		queueNext() ;
		
		return p ;
	}

	private void queueNext() {

		try {
			nextPage=toPage(iter.next()) ;

			if (type != null) {
				while (nextPage.getType() != type)
					nextPage = toPage(iter.next());
			}
		} catch (NoSuchElementException e) {
			nextPage = null ;
		}
	}

	private Page toPage(WEntry<Integer,DbPage> e) {
		if (e== null)
			return null ;
		else
			return Page.createPage(env, e.getKey(), e.getValue()) ;
	}

	/*
	public static void main(String[] args) throws Exception {
		
		DecimalFormat df = new DecimalFormat("0.000") ;

		if (args.length != 1) {		
			System.out.println("Please specify a directory containing a fully prepared Wikipedia database") ;
			return ;
		}

		File envDir = new File(args[0]) ;

		Wikipedia wikipedia = new Wikipedia(envDir) ;
		
		ProgressTracker tracker = new ProgressTracker(wikipedia.getEnvironment().getDbPage().getCount(), "Iterating pages", PageIterator.class) ;

		Iterator<Page> iter = wikipedia.getPageIterator(PageType.article) ;
		
		
		
		int count = 0 ;
		
		while (iter.hasNext()) {
			tracker.update() ;
			Page p = iter.next() ;
			
			if (count%1000 == 0) {
				System.out.println(p + " [" + p.getType() + "] - " + df.format(tracker.getTaskProgress())) ;
				
			}
			
			count++ ;
		}
		
		System.out.println(count) ;
	}*/
	
	public void close() {
		iter.close();
		
	}
}

