package org.pentaho.di.repository.jcr;

import org.pentaho.di.core.logging.LogChannel;

public class JCRRepositoryBaseDelegate {

	protected JCRRepository repository;
	protected LogChannel log;
	
	@SuppressWarnings("unused")
	public JCRRepositoryBaseDelegate(JCRRepository repository) {
		this.repository = repository;
		log = new LogChannel(this);
	}

}
