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

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;

public class SlaveDelegate extends AbstractDelegate implements ITransformer, SharedObjectAssembler<SlaveServer>, java.io.Serializable {

  private static final long serialVersionUID = -8084266831877112729L; /* EESOURCE: UPDATE SERIALVERUID */

  private static final String NODE_ROOT = "Slave"; //$NON-NLS-1$

  private static final String PROP_PASSWORD = "PASSWORD"; //$NON-NLS-1$

  private static final String PROP_USERNAME = "USERNAME"; //$NON-NLS-1$

  private static final String PROP_PORT = "PORT"; //$NON-NLS-1$

  private static final String PROP_HOST_NAME = "HOST_NAME"; //$NON-NLS-1$

  private static final String PROP_PROXY_HOST_NAME = "PROXY_HOST_NAME"; //$NON-NLS-1$

  private static final String PROP_PROXY_PORT = "PROXY_PORT"; //$NON-NLS-1$

  private static final String PROP_WEBAPP_NAME = "WEBAPP_NAME"; //$NON-NLS-1$

  private static final String PROP_NON_PROXY_HOSTS = "NON_PROXY_HOSTS"; //$NON-NLS-1$

  private static final String PROP_MASTER = "MASTER"; //$NON-NLS-1$


  // ~ Instance fields =================================================================================================

  private PurRepository repo;

  // ~ Constructors ====================================================================================================

  public SlaveDelegate(final PurRepository repo) {
    super();
    this.repo = repo;
  }

  public RepositoryElementInterface dataNodeToElement(DataNode rootNode) throws KettleException {
    SlaveServer slaveServer = new SlaveServer();
    dataNodeToElement(rootNode, slaveServer);
    return slaveServer;
  }

  public void dataNodeToElement(DataNode rootNode, RepositoryElementInterface element) throws KettleException {
    SlaveServer slaveServer = (SlaveServer) element;
    slaveServer.setHostname(getString(rootNode, PROP_HOST_NAME));
    slaveServer.setPort(getString(rootNode, PROP_PORT));
    slaveServer.setUsername(getString(rootNode, PROP_USERNAME));
    slaveServer.setPassword(Encr.decryptPasswordOptionallyEncrypted(getString(rootNode, PROP_PASSWORD)));
    slaveServer.setProxyHostname(getString(rootNode, PROP_PROXY_HOST_NAME));
    slaveServer.setProxyPort(getString(rootNode, PROP_PROXY_PORT));
    slaveServer.setWebAppName(getString(rootNode, PROP_WEBAPP_NAME));
    slaveServer.setNonProxyHosts(getString(rootNode, PROP_NON_PROXY_HOSTS));
    slaveServer.setMaster(rootNode.getProperty(PROP_MASTER).getBoolean());
  }

  public DataNode elementToDataNode(RepositoryElementInterface element) throws KettleException {
    SlaveServer slaveServer = (SlaveServer) element;
    DataNode rootNode = new DataNode(NODE_ROOT);

    /*
    // Check for naming collision
    ObjectId slaveId = repo.getSlaveID(slaveServer.getName());
    if (slaveId != null && slaveServer.getObjectId()!=null && !slaveServer.getObjectId().equals(slaveId)) {
      // We have a naming collision, abort the save
      throw new KettleException("Failed to save object to repository. Object [" + slaveServer.getName()
          + "] already exists.");
    }
    */

    // Create or version a new slave node
    //
    rootNode.setProperty(PROP_HOST_NAME, slaveServer.getHostname());
    rootNode.setProperty(PROP_PORT, slaveServer.getPort());
    rootNode.setProperty(PROP_WEBAPP_NAME, slaveServer.getWebAppName());
    rootNode.setProperty(PROP_USERNAME, slaveServer.getUsername());
    rootNode.setProperty(PROP_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(slaveServer.getPassword()));
    rootNode.setProperty(PROP_PROXY_HOST_NAME, slaveServer.getProxyHostname());
    rootNode.setProperty(PROP_PROXY_PORT, slaveServer.getProxyPort());
    rootNode.setProperty(PROP_NON_PROXY_HOSTS, slaveServer.getNonProxyHosts());
    rootNode.setProperty(PROP_MASTER, slaveServer.isMaster());
    return rootNode;
  }

  protected Repository getRepository() {
    return repo;
  }
  
  public SlaveServer assemble(RepositoryFile file, NodeRepositoryFileData data, VersionSummary version)
      throws KettleException {
    SlaveServer slaveServer = (SlaveServer) dataNodeToElement(data.getNode());
    slaveServer.setName(file.getTitle());
    slaveServer.setObjectId(new StringObjectId(file.getId().toString()));
    slaveServer.setObjectRevision(repo.createObjectRevision(version));
    slaveServer.clearChanged();
    return slaveServer;
  }
}
