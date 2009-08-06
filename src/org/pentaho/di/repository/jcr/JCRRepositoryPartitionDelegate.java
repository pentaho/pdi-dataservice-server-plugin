package org.pentaho.di.repository.jcr;

import javax.jcr.Node;
import javax.jcr.version.Version;

import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;

public class JCRRepositoryPartitionDelegate extends JCRRepositoryBaseDelegate {

	private static final String	PARTITION_PROPERTY_PREFIX	= "PARTITION_#";

	public JCRRepositoryPartitionDelegate(JCRRepository repository) {
		super(repository);
	}

	public void savePartitionSchema(RepositoryElementInterface element, String versionComment, ProgressMonitorListener monitor) throws KettleException {
		try {
			PartitionSchema partitionSchema = (PartitionSchema) element;

			Node node = repository.createOrVersionNode(element, versionComment);
	
	        node.setProperty("DYNAMIC_DEFINITION", partitionSchema.isDynamicallyDefined());
	        node.setProperty("PARTITIONS_PER_SLAVE", partitionSchema.getNumberOfPartitionsPerSlave());

	        // Save the cluster-partition relationships
			//
	        node.setProperty("NR_PARTITIONS", partitionSchema.getPartitionIDs().size());
			for (int i=0;i<partitionSchema.getPartitionIDs().size();i++)
			{
				node.setProperty(PARTITION_PROPERTY_PREFIX+i, partitionSchema.getPartitionIDs().get(i));
			}
	        
			repository.getSession().save();
			Version version = node.checkin();
			partitionSchema.setObjectRevision(repository.getObjectRevision(version));
			partitionSchema.setObjectId(new StringObjectId(node.getUUID()));
			
		}catch(Exception e) {
			throw new KettleException("Unable to save partition schema ["+element+"] in the repository", e);
		}
	}

	public PartitionSchema loadPartitionSchema(ObjectId partitionSchemaId, String versionLabel) throws KettleException {
		try {
			
			Node node = repository.getSession().getNodeByUUID(partitionSchemaId.getId());
			Version version = repository.getVersion(node, versionLabel);
			Node partitionNode = repository.getVersionNode(version);
			
			PartitionSchema partitionSchema = new PartitionSchema();
			
			partitionSchema.setName( repository.getObjectName(partitionNode));
			partitionSchema.setDescription( repository.getObjectDescription(partitionNode));
			
			// Grab the Version comment...
			//
			partitionSchema.setObjectRevision( repository.getObjectRevision(version) );

			// Get the unique ID
			//
			ObjectId objectId = new StringObjectId(node.getUUID());
			partitionSchema.setObjectId(objectId);
						
			// The metadata...
			//
	        partitionSchema.setDynamicallyDefined( repository.getPropertyBoolean(partitionNode, "DYNAMIC_DEFINITION") );
	        partitionSchema.setNumberOfPartitionsPerSlave( repository.getPropertyString(partitionNode, "PARTITIONS_PER_SLAVE") );

	        // Save the cluster-partition relationships
			//
	        int nrPartitions = (int) repository.getPropertyLong(partitionNode, "NR_PARTITIONS");
			for (int i=0;i<nrPartitions;i++)
			{
				String partitionId = partitionNode.getProperty(PARTITION_PROPERTY_PREFIX+i).getString();
				partitionSchema.getPartitionIDs().add(partitionId);
			}

			return partitionSchema;
		}
		catch(Exception e) {
			throw new KettleException("Unable to load database from object ["+partitionSchemaId+"]", e);
		}
	}
}
