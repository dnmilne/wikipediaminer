/*
 *    Position.java
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

/**
 * @author David Milne
 *
 * This class represents a position in a linear sequence, with a start index and an end index.
 */
public class Position {

	private int start ;
	private int end ;

	/**
	 * Initializes a new position with the given start and end indexes.
	 * 
	 * @param start the start index of this position
	 * @param end the end index of this posion
	 */
	public Position(int start, int end) {
		this.start = start ;
		this.end = end ;
	}

	/**
	 * Identifies whether this position overlaps with another one.
	 * 
	 * @param pos
	 * @return true if the positions overlap, false otherwise.
	 */
	public boolean overlaps(Position pos) {
		return !(end <= pos.start || start >= pos.end) ;
	}

	/**
	 * @return the start index of this position
	 */
	public int getStart() {
		return start;
	}

	/**
	 * @return the end index of this position
	 */
	public int getEnd() {
		return end;
	}
	
	public String toString() {
		return "(" + start + "," + end + ")" ;		
	}
}
