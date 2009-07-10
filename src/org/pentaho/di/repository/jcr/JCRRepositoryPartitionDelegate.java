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
			ObjectId id = new StringObjectId(node.getUUID());
	
	        node.setProperty("DYNAMIC_DEFINITION", partitionSchema.isDynamicallyDefined());
	        node.setProperty("PARTITIONS_PER_SLAVE", partitionSchema.getNumberOfPartitionsPerSlave());

	        // Save the cluster-partition relationships
			//
			for (int i=0;i<partitionSchema.getPartitionIDs().size();i++)
			{
				node.setProperty(PARTITION_PROPERTY_PREFIX+i, partitionSchema.getPartitionIDs().get(i));
			}
	        
			Version version = node.checkin();
			partitionSchema.setObjectId(id);
			partitionSchema.setObjectVersion(repository.getObjectVersion(version));
			
		}catch(Exception e) {
			throw new KettleException("Unable to save partition schema ["+element+"] in the repository", e);
		}
	}
}
