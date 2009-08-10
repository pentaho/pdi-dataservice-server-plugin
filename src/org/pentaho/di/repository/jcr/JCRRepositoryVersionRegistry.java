package org.pentaho.di.repository.jcr;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.NodeIterator;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectVersion;
import org.pentaho.di.repository.RepositoryVersionRegistry;
import org.pentaho.di.repository.SimpleObjectVersion;

public class JCRRepositoryVersionRegistry implements RepositoryVersionRegistry {

	public static final String NODE_VERSIONS_NODE_NAME = "__PDI_VERSIONS__";

	public static final String	PROPERTY_PREVIOUS_VERSION      = "PreviousVersion";
	public static final String	PROPERTY_PLANNED_RELEASE_DATE  = "PlannedReleaseDate";

	private JCRRepository	repository;

	private Node	versionsNodeFolder;

	public JCRRepositoryVersionRegistry(JCRRepository repository) {
		this.repository = repository;
	}
	
	public void addVersion(ObjectVersion version) throws KettleException {
		try {
			// Simply create new node...
			//
			Node node = versionsNodeFolder.addNode(repository.sanitizeNodeName(version.getLabel()));
			node.addMixin(JCRRepository.MIX_REFERENCEABLE);

			update(node, version);
			
			repository.getSession().save();
			
		} catch(Exception e) {
			throw new KettleException("Unable to create a new version node ["+version+"]", e);
		}
	}
	
	public void updateVersion(ObjectVersion version) throws KettleException {
		try {
			Node node = findNode(version.getLabel());
			update(node, version);
			repository.getSession().save();
		} catch(Exception e) {
			throw new KettleException("Unable to update version node ["+version+"]", e);
		}
	}
	
	public void removeVersion(String label) throws KettleException {
		try {
			Node node = findNode(label);
			node.remove();
			repository.getSession().save();
		} catch(Exception e) {
			throw new KettleException("Unable to remove version node ["+label+"]", e);
		}
	}
	
	private Node findNode(String label) throws KettleException {
		try {
			NodeIterator nodes = versionsNodeFolder.getNodes();
			while (nodes.hasNext()) {
				Node node = nodes.nextNode();
				
				String name= repository.getPropertyString(node, JCRRepository.PROPERTY_NAME);
				if (label.equals(name)) {
					return node;
				}
			}
			throw new KettleException("Unable to find the version with label ["+label+"] in the version registry");

		} catch(Exception e) {
			throw new KettleException("Error finding object version node from the version registry", e);
		}
	}

	private void update(Node node, ObjectVersion version) throws KettleException {
		try {
			node.setProperty(JCRRepository.PROPERTY_NAME, version.getLabel());
			node.setProperty(JCRRepository.PROPERTY_DESCRIPTION, version.getDescription());
			node.setProperty(PROPERTY_PREVIOUS_VERSION, version.getPreviousVersion());

			Calendar plannedReleaseDate = null;
			if (version.getPlannedReleaseDate()!=null) {
				plannedReleaseDate = Calendar.getInstance();
				plannedReleaseDate.setTime(version.getPlannedReleaseDate());
			}
			node.setProperty(PROPERTY_PLANNED_RELEASE_DATE, plannedReleaseDate);
		}
		catch(Exception e) {
			throw new KettleException("Unable to set properties on version node ["+version+"]", e);
		}
	}

	public List<ObjectVersion> getVersions() throws KettleException {
		
		try {
			List<ObjectVersion> list = new ArrayList<ObjectVersion>();

			NodeIterator nodes = versionsNodeFolder.getNodes();
			while (nodes.hasNext()) {
				Node node = nodes.nextNode();
				
				String name = repository.getPropertyString(node, JCRRepository.PROPERTY_NAME);
				String description = repository.getPropertyString(node, JCRRepository.PROPERTY_DESCRIPTION);
				String previousLabel = repository.getPropertyString(node, PROPERTY_PREVIOUS_VERSION);
				Date plannedReleaseDate = repository.getPropertyDate(node, PROPERTY_PLANNED_RELEASE_DATE);

				list.add(new SimpleObjectVersion(name, description, previousLabel, plannedReleaseDate));
			}
			
			return list;
		} catch(Exception e) {
			throw new KettleException("Unable to load the versions list from the version registry", e);
		}
	}
	
	public JCRRepository getRepository() {
		return repository;
	}

	public void checkVersionsFolder() throws KettleException {
		try {
			versionsNodeFolder = null;
			NodeIterator nodes = repository.getRootNode().getNodes();
			while (nodes.hasNext() && versionsNodeFolder==null) {
				Node node = nodes.nextNode();
				if (node.getName().equals(NODE_VERSIONS_NODE_NAME)) {
					versionsNodeFolder = node;
				}
			}
			if (versionsNodeFolder==null) {
				versionsNodeFolder = repository.getRootNode().addNode(NODE_VERSIONS_NODE_NAME, "nt:unstructured");
			}
		}
		catch(Exception e) {
			throw new KettleException("Unable to find or create the versions node ["+NODE_VERSIONS_NODE_NAME+"] in the root node", e);
		}

	}
}
