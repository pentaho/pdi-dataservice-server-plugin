package org.pentaho.di.repository.jcr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.version.Version;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;

public class JCRRepositoryDatabaseDelegate extends JCRRepositoryBaseDelegate {

	private static final String DB_ATTRIBUTE_PREFIX = "DB_ATTR_";
	
	public JCRRepositoryDatabaseDelegate(JCRRepository repository) {
		super(repository);
	}

	public void saveDatabaseMeta(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {

			DatabaseMeta databaseMeta = (DatabaseMeta) element;
			
			// First see if a node with the same name already exists. If so, version it, otherwise create it...
			//
			Node node = repository.createOrVersionNode(element, versionComment);
			ObjectId id = new StringObjectId(node.getUUID());

			// Then the basic db information
			//
			node.setProperty("TYPE", DatabaseMeta.getDatabaseTypeCode(databaseMeta.getDatabaseType()));
			node.setProperty("CONTYPE", DatabaseMeta.getAccessTypeDesc(databaseMeta.getAccessType()));
			node.setProperty("HOST_NAME", databaseMeta.getHostname());
			node.setProperty("DATABASE_NAME", databaseMeta.getDatabaseName());
			node.setProperty("PORT", new Long(Const.toInt(databaseMeta.getDatabasePortNumberString(), -1)));
			node.setProperty("USERNAME", databaseMeta.getUsername());
			node.setProperty("PASSWORD", databaseMeta.getPassword());
			node.setProperty("SERVERNAME", databaseMeta.getServername());
			node.setProperty("DATA_TBS", databaseMeta.getDataTablespace());
			node.setProperty("INDEX_TBS", databaseMeta.getIndexTablespace());

			
			// Before we store the extra properties, clear them...
			//
			PropertyIterator properties = node.getProperties(DB_ATTRIBUTE_PREFIX+"*");
			while (properties.hasNext()) {
				Property property = properties.nextProperty();
				node.setProperty(property.getName(), (String)null); // removes
			}
						
            // Now store all the attributes set on the database connection...
            // 
            Properties attributes = databaseMeta.getAttributes();
            Enumeration<Object> keys = databaseMeta.getAttributes().keys();
            while (keys.hasMoreElements())
            {
                String code = (String) keys.nextElement();
                String attribute = (String)attributes.get(code);
                
                // Save this attribute
                //
                node.setProperty(DB_ATTRIBUTE_PREFIX+code, attribute);
            }

            repository.getSession().save();
            Version version = node.checkin();
            
            databaseMeta.setObjectVersion(repository.getObjectVersion(version));
            databaseMeta.setObjectId(id);
            
		} catch (Exception e) {
			throw new KettleException("Unable to save database connection ["+element+"] in the repository", e);
		}
		
	}

	public DatabaseMeta loadDatabaseMeta(ObjectId databaseId, String versionLabel) throws KettleException {
		try {
			Version version = repository.getVersion(repository.getSession().getNodeByUUID(databaseId.getId()), versionLabel);
			Node node = repository.getVersionNode(version);
			
			DatabaseMeta databaseMeta = new DatabaseMeta();
			
			databaseMeta.setDescription(repository.getPropertyString(node, JCRRepository.PROPERTY_DESCRIPTION) );
			
			databaseMeta.setName( repository.getObjectName(node) );
			databaseMeta.setDatabaseType( repository.getPropertyString(node, "TYPE") );
			databaseMeta.setAccessType( DatabaseMeta.getAccessType(repository.getPropertyString(node, "CONTYPE")) );
			databaseMeta.setHostname( repository.getPropertyString(node, "HOST_NAME") );
			databaseMeta.setDBName( repository.getPropertyString(node, "DATABASE_NAME") );
			databaseMeta.setDBPort( repository.getPropertyString(node, "PORT") );
			databaseMeta.setUsername( repository.getPropertyString(node, "USERNAME") );
			databaseMeta.setPassword( repository.getPropertyString(node, "PASSWORD") );
			databaseMeta.setServername( repository.getPropertyString(node, "SERVERNAME") );
			databaseMeta.setDataTablespace( repository.getPropertyString(node, "DATA_TBS") );
			databaseMeta.setIndexTablespace( repository.getPropertyString(node, "INDEX_TBS") );
			

            // Also, load all the properties we can find...
			Collection<String[]> attrs = getDatabaseAttributes(node);
            for (String[] pair : attrs)
            {
                String code = pair[0];
                String attribute = pair[1];
                databaseMeta.getAttributes().put(code, Const.NVL(attribute, ""));
            }

            databaseMeta.setObjectId(databaseId);
			databaseMeta.setObjectVersion(repository.getObjectVersion(version));
			databaseMeta.clearChanged();			
			
			return databaseMeta;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load database from object ["+databaseId+"]", e);
		}
	}
	
	public Collection<String[]> getDatabaseAttributes(Node node) throws RepositoryException
    {
    	List<String[]> attrs = new ArrayList<String[]>();
    	PropertyIterator properties = node.getProperties(DB_ATTRIBUTE_PREFIX+"*");
    	while (properties.hasNext()) {
    		Property property = properties.nextProperty();
    		
    		String code = property.getName().substring(DB_ATTRIBUTE_PREFIX.length());
    		String attribute = property.getString();
    		
    		attrs.add(new String[] { code, attribute, });
    	}
    	return attrs;
    }

	public ObjectId renameDatabase(ObjectId databaseId, String newname) throws KettleException {
		try { 
			Node node = repository.getSession().getNodeByUUID(databaseId.getId());

			// Change the name in the properties...
			//
			node.checkout();
			node.setProperty(JCRRepository.PROPERTY_NAME, newname);

			// Now change the name of the node itself with a move
			//
			String oldPath = node.getPath();
			String newPath = repository.calcNodePath(null, newname, JCRRepository.EXT_DATABASE);
			
			repository.getSession().move(oldPath, newPath);
			repository.getSession().save();
			node.checkin();
			
			return databaseId; // same ID, nothing changed
		} catch(Exception e) {
			throw new KettleException("Unable to rename database with id ["+databaseId+"] to ["+newname+"]", e);
		}
	}

	public void deleteDatabaseMeta(String databaseName) throws KettleException {
		try {
			String path = repository.calcRelativeNodePath(null, databaseName, JCRRepository.EXT_DATABASE);
			Node databaseNode = repository.getRootNode().getNode(path);
			repository.deleteObject(new StringObjectId(databaseNode.getUUID()), RepositoryObjectType.DATABASE);
		} catch(Exception e) {
			throw new KettleException("Unable to delete database connection with name ["+databaseName+"]", e);
		}
	}
    
	
}
