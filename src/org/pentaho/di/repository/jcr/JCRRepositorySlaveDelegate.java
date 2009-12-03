package org.pentaho.di.repository.jcr;

import java.util.List;

import javax.jcr.Node;
import javax.jcr.version.Version;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.jcr.util.JCRObjectRevision;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

public class JCRRepositorySlaveDelegate extends JCRRepositoryBaseDelegate {
	
	private static final String	CLUSTER_SLAVE_PREFIX	= "__SLAVE_SERVER_#";

	public JCRRepositorySlaveDelegate(JCRRepository repository) {
		super(repository);
	}

	public void saveSlaveServer(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			SlaveServer slaveServer = (SlaveServer)element;

			// Create or version a new slave node
			//
			Node slaveNode = repository.createOrVersionNode(element, versionComment);
	    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
	    if (dscContent.getSubject()!=null){
        slaveNode.setProperty("HOST_NAME", slaveServer.getHostname());
        slaveNode.setProperty("PORT", slaveServer.getPort());
        slaveNode.setProperty("USERNAME", slaveServer.getUsername());
        slaveNode.setProperty("PASSWORD", Encr.encryptPasswordIfNotUsingVariables(slaveServer.getPassword()));
        slaveNode.setProperty("PROXY_HOST_NAME", slaveServer.getProxyHostname());
        slaveNode.setProperty("PROXY_PORT", slaveServer.getProxyPort());
        slaveNode.setProperty("NON_PROXY_HOSTS", slaveServer.getNonProxyHosts());
        slaveNode.setProperty("MASTER", slaveServer.isMaster());
        
        repository.getSession().save();
        Version version = slaveNode.checkin();
        slaveServer.setObjectRevision( new JCRObjectRevision(version, versionComment, repository.getUserInfo().getLogin()) );
        slaveServer.setObjectId(new StringObjectId(slaveNode.getUUID()));
	    }
		} catch(Exception e) {
			throw new KettleException("Unable to save slave server ["+element+"] in the repository", e);
		}
	}

	public SlaveServer loadSlaveServer(ObjectId id_slave_server, String versionLabel) throws KettleException {
		
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
    if (dscContent.getHolder()==null){
      return null;
    }

	  try {
			Node node = repository.getSession().getNodeByUUID(id_slave_server.getId());
			Version version = repository.getVersion(node, versionLabel);
			Node slaveNode = repository.getVersionNode(version);
			
			SlaveServer slaveServer = new SlaveServer();
			
			slaveServer.setName( repository.getObjectName(slaveNode));
			slaveServer.setDescription( repository.getObjectDescription(slaveNode));
			
			// Grab the Version comment...
			//
			slaveServer.setObjectRevision( repository.getObjectRevision(version) );

			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			slaveServer.setObjectId(objectId);
						
			// The metadata...
			//
			slaveServer.setHostname( repository.getPropertyString(slaveNode, "HOST_NAME") );
	        slaveServer.setPort( repository.getPropertyString(slaveNode, "PORT") );
	        slaveServer.setUsername( repository.getPropertyString(slaveNode, "USERNAME") );
	        slaveServer.setPassword( Encr.decryptPasswordOptionallyEncrypted( repository.getPropertyString(slaveNode, "PASSWORD")) );
	        slaveServer.setProxyHostname( repository.getPropertyString(slaveNode, "PROXY_HOST_NAME") );
	        slaveServer.setProxyPort( repository.getPropertyString(slaveNode, "PROXY_PORT") );
	        slaveServer.setNonProxyHosts( repository.getPropertyString(slaveNode, "NON_PROXY_HOSTS") );
	        slaveServer.setMaster( repository.getPropertyBoolean(slaveNode, "MASTER") );

			return slaveServer;
		} catch(Exception e) {
			throw new KettleException("Unable to load slave server with id ["+id_slave_server+"] from the repository", e);
		}
	}

	public void saveClusterSchema(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			ClusterSchema clusterSchema = (ClusterSchema)element;
	
			// Create or version a new slave node
			//
			Node clusterNode = repository.createOrVersionNode(element, versionComment);

			// save the properties...
	        //
	        clusterNode.setProperty("BASE_PORT", clusterSchema.getBasePort());
	        clusterNode.setProperty("SOCKETS_BUFFER_SIZE", clusterSchema.getSocketsBufferSize());
	        clusterNode.setProperty("SOCKETS_FLUSH_INTERVAL", clusterSchema.getSocketsFlushInterval());
	        clusterNode.setProperty("SOCKETS_COMPRESSED", clusterSchema.isSocketsCompressed());
	        clusterNode.setProperty("DYNAMIC", clusterSchema.isDynamic());

	        // Also save the used slave server references.
	        //
	        clusterNode.setProperty("NR_SLAVES", clusterSchema.getSlaveServers().size());
	        for (int i=0;i<clusterSchema.getSlaveServers().size();i++)
	        {
	            SlaveServer slaveServer = clusterSchema.getSlaveServers().get(i);
	            Node slaveNode = repository.getSession().getNodeByUUID( slaveServer.getObjectId().getId() );
	            
	            // Save the slave server by reference, this way it becomes impossible to delete the slave by accident when still in use.
	            //
	            clusterNode.setProperty(CLUSTER_SLAVE_PREFIX+i, slaveNode);
	        }
	        
			// A little admin work...
			//
	        PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
	        if (dscContent.getIssuer() != null){
  	        repository.getSession().save();
  	        Version version = clusterNode.checkin();
  	        clusterSchema.setObjectRevision( new JCRObjectRevision(version, versionComment, repository.getUserInfo().getLogin()) );
  	        clusterSchema.setObjectId(new StringObjectId(clusterNode.getUUID()));
	        }
	
		} catch(Exception e) {
			throw new KettleException("Unable to save cluster schema ["+element+"] in the repository", e);
		}
	}

	public ClusterSchema loadClusterSchema(ObjectId clusterSchemaId, List<SlaveServer> slaveServers, String versionLabel) throws KettleException {
		try {
			Node node = repository.getSession().getNodeByUUID(clusterSchemaId.getId());
			Version version = repository.getVersion(node, versionLabel);
			Node clusterNode = repository.getVersionNode(version);
			
			ClusterSchema clusterSchema = new ClusterSchema();
			
			clusterSchema.setName( repository.getObjectName(clusterNode));
			clusterSchema.setDescription( repository.getObjectDescription(clusterNode));
			
			// Grab the Version comment...
			//
			clusterSchema.setObjectRevision( repository.getObjectRevision(version) );

	    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
	    if (dscContent.getSubject()==null){
	      return null;
	    }
			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			clusterSchema.setObjectId(objectId);
						
			// The metadata...
			//
			clusterSchema.setBasePort( repository.getPropertyString(clusterNode, "BASE_PORT") );
	        clusterSchema.setSocketsBufferSize( repository.getPropertyString(clusterNode, "SOCKETS_BUFFER_SIZE") );
	        clusterSchema.setSocketsFlushInterval( repository.getPropertyString(clusterNode, "SOCKETS_FLUSH_INTERVAL") );
	        clusterSchema.setSocketsCompressed( repository.getPropertyBoolean(clusterNode, "SOCKETS_COMPRESSED") );
	        clusterSchema.setDynamic( repository.getPropertyBoolean(clusterNode, "DYNAMIC") );
	        
	        // The slaves...
	        //
	        int nrSlaves = (int) clusterNode.getProperty("NR_SLAVES").getLong();
	        for (int i=0;i<nrSlaves;i++) {
	        	Node slaveNode = clusterNode.getProperty(CLUSTER_SLAVE_PREFIX+i).getNode();
	        	clusterSchema.getSlaveServers().add( SlaveServer.findSlaveServer(slaveServers, new StringObjectId(slaveNode.getUUID())) );
	        }

			return clusterSchema;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load cluster schema from object ["+clusterSchemaId+"]", e);
		}
	}

	public void deleteClusterSchema(ObjectId clusterId) throws KettleException {
		try {
			repository.deleteObject(clusterId, RepositoryObjectType.CLUSTER_SCHEMA);
		} catch(Exception e) {
			throw new KettleException("Unable to delete cluster schema with id ["+clusterId+"]", e);
		}
	}

	public void deleteSlave(ObjectId slaveServerId) throws KettleException {
		try {
			repository.deleteObject(slaveServerId, RepositoryObjectType.SLAVE_SERVER);
		} catch(Exception e) {
			throw new KettleException("Unable to delete slave server with id ["+slaveServerId+"]", e);
		}
	}	
}
