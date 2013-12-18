package org.wikipedia.miner.extract.model;

import java.util.Date;

import org.wikipedia.miner.extract.util.SiteInfo.Namespace;
import org.wikipedia.miner.model.Page.PageType;

public class DumpPage {

	private int id ;
	private Namespace namespace ;
	private PageType type ;

	private String title ;
	private String markup ;
	private String target ;
	private Date lastEdited ;
	
	public DumpPage(int id, Namespace namespace, PageType type, String title, String markup, String target, Date lastEdited) {
		
		this.id = id ;
		this.namespace = namespace ;
		
		this.type = type ;
		
		this.title = title ;
		this.markup = markup ;
		this.target = target ;
		
		this.lastEdited = lastEdited ;
	}

	public int getId() {
		return id;
	}

	public String getMarkup() {
		return markup;
	}

	public Namespace getNamespace() {
		return namespace;
	}

	public String getTitle() {
		return title;
	}

	public PageType getType() {
		return type;
	}
	
	public String getTarget() {
		return target ;
	}
	
	public Date getLastEdited() {
		return lastEdited ;
	}
	
	@Override
	public String toString() {
		return id + ":" + title ;
	}
}
