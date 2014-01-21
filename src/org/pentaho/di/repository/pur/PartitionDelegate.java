/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.repository.pur;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.DataProperty;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;

public class PartitionDelegate extends AbstractDelegate implements ITransformer, SharedObjectAssembler<PartitionSchema>, java.io.Serializable {

  private static final long serialVersionUID = -6069812592810099251L; /* EESOURCE: UPDATE SERIALVERUID */

  private static final String NODE_ROOT = "partitionSchema"; //$NON-NLS-1$

  private static final String PROP_DYNAMIC_DEFINITION = "DYNAMIC_DEFINITION"; //$NON-NLS-1$

  private static final String PROP_PARTITIONS_PER_SLAVE = "PARTITIONS_PER_SLAVE"; //$NON-NLS-1$

  private static final String NODE_ATTRIBUTES = "attributes"; //$NON-NLS-1$

  private static final String PROP_NB_PARTITION_SCHEMA = "NB_PARTITION_SCHEMA"; //$NON-NLS-1$
  // ~ Instance fields =================================================================================================

  private PurRepository repo;

  // ~ Constructors ====================================================================================================

  public PartitionDelegate(final PurRepository repo) {
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
    PartitionSchema partitionSchema = (PartitionSchema) element;
    partitionSchema.setDynamicallyDefined(rootNode.getProperty(PROP_DYNAMIC_DEFINITION).getBoolean());
    partitionSchema.setNumberOfPartitionsPerSlave(getString(rootNode, PROP_PARTITIONS_PER_SLAVE));
    // Also, load all the properties we can find...

    DataNode attrNode = rootNode.getNode(NODE_ATTRIBUTES);
    long partitionSchemaSize = attrNode.getProperty(PROP_NB_PARTITION_SCHEMA).getLong();

    for(int i = 0; i < partitionSchemaSize;i++) {
      DataProperty property = attrNode.getProperty(String.valueOf(i));
      partitionSchema.getPartitionIDs().add(Const.NVL(property.getString(), ""));
    }

  }

  public DataNode elementToDataNode(RepositoryElementInterface element) throws KettleException {
    PartitionSchema partitionSchema = (PartitionSchema) element;
    DataNode rootNode = new DataNode(NODE_ROOT);

    // Check for naming collision
    ObjectId partitionId = repo.getPartitionSchemaID(partitionSchema.getName());
    if (partitionId != null && !partitionSchema.getObjectId().equals(partitionId)) {
      // We have a naming collision, abort the save
      throw new KettleException("Failed to save object to repository. Object [" + partitionSchema.getName()
          + "] already exists.");
    }
    rootNode.setProperty(PROP_DYNAMIC_DEFINITION, partitionSchema.isDynamicallyDefined());
    rootNode.setProperty(PROP_PARTITIONS_PER_SLAVE, partitionSchema.getNumberOfPartitionsPerSlave());

    // Save the cluster-partition relationships
    DataNode attrNode = rootNode.addNode(NODE_ATTRIBUTES);
    attrNode.setProperty(PROP_NB_PARTITION_SCHEMA, partitionSchema.getPartitionIDs().size());
    for (int i = 0; i < partitionSchema.getPartitionIDs().size(); i++) {
      attrNode.setProperty(String.valueOf(i), partitionSchema.getPartitionIDs().get(i));
    }
    return rootNode;
  }

  public PartitionSchema assemble(RepositoryFile file, NodeRepositoryFileData data, VersionSummary version)
      throws KettleException {
    PartitionSchema partitionSchema = (PartitionSchema) dataNodeToElement(data.getNode());
    partitionSchema.setName(file.getTitle());
    partitionSchema.setObjectId(new StringObjectId(file.getId().toString()));
    partitionSchema.setObjectRevision(repo.createObjectRevision(version));
    partitionSchema.clearChanged();
    return partitionSchema;
  }
}
