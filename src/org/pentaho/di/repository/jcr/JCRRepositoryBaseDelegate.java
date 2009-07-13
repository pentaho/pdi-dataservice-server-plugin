package org.pentaho.di.repository.jcr;

import org.pentaho.di.core.logging.LogWriter;

public class JCRRepositoryBaseDelegate {

	protected JCRRepository repository;
	protected LogWriter log;
	
	public JCRRepositoryBaseDelegate(JCRRepository repository) {
		this.repository = repository;
		this.log = LogWriter.getInstance();
	}

}
