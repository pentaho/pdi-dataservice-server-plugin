package org.pentaho.di.repository.pur;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.platform.repository.pcr.data.node.DataNode;
import org.pentaho.platform.repository.pcr.data.node.DataProperty;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

public class PartitionDelegate extends AbstractDelegate implements ITransformer {

  private static final String NODE_ROOT = "partitionSchema"; //$NON-NLS-1$

  private static final String PROP_DYNAMIC_DEFINITION = "DYNAMIC_DEFINITION"; //$NON-NLS-1$

  private static final String PROP_PARTITIONS_PER_SLAVE = "PARTITIONS_PER_SLAVE"; //$NON-NLS-1$

  private static final String NODE_ATTRIBUTES = "attributes"; //$NON-NLS-1$

  // ~ Instance fields =================================================================================================

  private Repository repo;

  // ~ Constructors ====================================================================================================

  public PartitionDelegate(final Repository repo) {
    super();
    this.repo = repo;
  }

  // ~ Methods =========================================================================================================

  public RepositoryElementInterface dataNodeToElement(DataNode rootNode) throws KettleException {
    PartitionSchema partitionSchema = new PartitionSchema();
    dataNodeToElement(rootNode, partitionSchema);
    return partitionSchema;
  }

  public void dataNodeToElement(DataNode rootNode, RepositoryElementInterface element) throws KettleException {
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());

    PartitionSchema partitionSchema = (PartitionSchema) element;
    if (dscContent.getExtra() != null) {
      partitionSchema.setName(rootNode.getProperty(PROP_NAME).getString());
    }
    partitionSchema.setDynamicallyDefined(rootNode.getProperty(PROP_DYNAMIC_DEFINITION).getBoolean());
    partitionSchema.setNumberOfPartitionsPerSlave(rootNode.getProperty(PROP_PARTITIONS_PER_SLAVE).getString());

    // Also, load all the properties we can find...

    DataNode attrNode = rootNode.getNode(NODE_ATTRIBUTES);
    int size = 0;

    for (DataProperty property : attrNode.getProperties()) {
      size++;
    }

    for(int i = 0; i < size;i++) {
      DataProperty property = attrNode.getProperty(String.valueOf(i));
      partitionSchema.getPartitionIDs().add(Const.NVL(property.getString(), ""));
    }

  }

  public DataNode elementToDataNode(RepositoryElementInterface element) throws KettleException {
    PartitionSchema partitionSchema = (PartitionSchema) element;
    DataNode rootNode = new DataNode(NODE_ROOT);

    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());

    // Check for naming collision
    ObjectId partitionId = repo.getPartitionSchemaID(partitionSchema.getName());
    if (partitionId != null && !partitionSchema.getObjectId().equals(partitionId)) {
      // We have a naming collision, abort the save
      throw new KettleException("Failed to save object to repository. Object [" + partitionSchema.getName()
          + "] already exists.");
    }
    rootNode.setProperty(PROP_NAME, partitionSchema.getName());
    rootNode.setProperty(PROP_DYNAMIC_DEFINITION, partitionSchema.isDynamicallyDefined());
    rootNode.setProperty(PROP_PARTITIONS_PER_SLAVE, partitionSchema.getNumberOfPartitionsPerSlave());

    // Save the cluster-partition relationships
    DataNode attrNode = rootNode.addNode(NODE_ATTRIBUTES);
    if (dscContent.getSubject() != null) {
      for (int i = 0; i < partitionSchema.getPartitionIDs().size(); i++) {
        attrNode.setProperty(String.valueOf(i), partitionSchema.getPartitionIDs().get(i));
      }
    }
    return rootNode;
  }
}
