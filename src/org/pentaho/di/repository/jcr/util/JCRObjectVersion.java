package org.pentaho.di.repository.jcr.util;

import java.util.Calendar;
import java.util.Date;

import javax.jcr.RepositoryException;
import javax.jcr.version.Version;

import org.pentaho.di.repository.ObjectVersion;

public class JCRObjectVersion implements ObjectVersion {

	private Version	 version;
	private String	 name;
	private Calendar created;
	private String	 comment;
	private String   login;

	public JCRObjectVersion(Version version, String comment, String login) throws RepositoryException {
		this.version = version;
		
		this.name = version.getName();
		this.created = version.getCreated();
		this.comment = comment;
		this.login = login;
	}

	public String getComment() {
		return comment;
	}

	public Date getCreationDate() {
		return created.getTime();
	}

	public String getName() {
		return name;
	}
	
	public Version getVersion() {
		return version;
	}
	
	public boolean equals(Object obj) {
		return name.equals(((JCRObjectVersion)obj).name);
	}
	
	public String getLogin() {
		return login;
	}
}
