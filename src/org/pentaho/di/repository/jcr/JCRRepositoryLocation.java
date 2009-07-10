package org.pentaho.di.repository.jcr;

public class JCRRepositoryLocation {
	private String url;
	
	public JCRRepositoryLocation(String url) {
		this.url = url;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
}
