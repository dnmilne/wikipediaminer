package org.wikipedia.miner.extract.model;

import org.wikipedia.miner.extract.util.SiteInfo.Namespace;

public class DumpLink {
	
	
	private String targetTitle ;
	private String targetSection ;
	private Namespace targetNamespace ;
	private String targetLanguage ;
	
	private String anchor ;
	
	public DumpLink(String targetLanguage, Namespace targetNamespace, String targetTitle, String targetSection, String anchor) {
		
		this.targetLanguage = targetLanguage ;
		this.targetNamespace = targetNamespace ;
		this.targetTitle = targetTitle ;
		this.targetSection = targetSection ;
		this.anchor = anchor ;
	}

	public String getAnchor() {
		return anchor;
	}

	public String getTargetLanguage() {
		return targetLanguage;
	}

	public Namespace getTargetNamespace() {
		return targetNamespace;
	}

	public String getTargetSection() {
		return targetSection;
	}

	public String getTargetTitle() {
		return targetTitle;
	}
}
