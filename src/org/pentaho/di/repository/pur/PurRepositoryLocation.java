/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

public class PurRepositoryLocation implements java.io.Serializable {

  private static final long serialVersionUID = 2380968812271105007L; /* EESOURCE: UPDATE SERIALVERUID */
	private String url;
	
	public PurRepositoryLocation(String url) {
		this.url = url;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
}
