package org.pentaho.di.repository.pur;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.platform.repository.pcr.data.node.DataNode;

import com.pentaho.commons.dsc.PentahoDscContent;
import com.pentaho.commons.dsc.PentahoLicenseVerifier;
import com.pentaho.commons.dsc.params.KParam;

public class SlaveDelegate extends AbstractDelegate implements ITransformer {

  private static final String NODE_ROOT = "Slave"; //$NON-NLS-1$

  private static final String PROP_PASSWORD = "PASSWORD"; //$NON-NLS-1$

  private static final String PROP_USERNAME = "USERNAME"; //$NON-NLS-1$

  private static final String PROP_PORT = "PORT"; //$NON-NLS-1$

  private static final String PROP_HOST_NAME = "HOST_NAME"; //$NON-NLS-1$

  private static final String PROP_PROXY_HOST_NAME = "PROXY_HOST_NAME"; //$NON-NLS-1$

  private static final String PROP_PROXY_PORT = "PROXY_PORT"; //$NON-NLS-1$

  private static final String PROP_NON_PROXY_HOSTS = "NON_PROXY_HOSTS"; //$NON-NLS-1$

  private static final String PROP_MASTER = "MASTER"; //$NON-NLS-1$


  // ~ Instance fields =================================================================================================

  private Repository repo;

  // ~ Constructors ====================================================================================================

  public SlaveDelegate(final Repository repo) {
    super();
    this.repo = repo;
  }

  public RepositoryElementInterface dataNodeToElement(DataNode rootNode) throws KettleException {
    SlaveServer slaveServer = new SlaveServer();
    dataNodeToElement(rootNode, slaveServer);
    return slaveServer;
  }

  public void dataNodeToElement(DataNode rootNode, RepositoryElementInterface element) throws KettleException {
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());
    SlaveServer slaveServer = (SlaveServer) element;
    if (dscContent.getExtra() != null) {
      slaveServer.setName(getString(rootNode, PROP_NAME));
      if (rootNode.hasProperty(PROP_DESCRIPTION)) {
        slaveServer.setDescription(getString(rootNode, PROP_DESCRIPTION));
      }
    }
    slaveServer.setHostname(getString(rootNode, PROP_HOST_NAME));
    slaveServer.setPort(getString(rootNode, PROP_PORT));
    slaveServer.setUsername(getString(rootNode, PROP_USERNAME));
    slaveServer.setPassword(Encr.decryptPasswordOptionallyEncrypted(getString(rootNode, PROP_PASSWORD)));
    slaveServer.setProxyHostname(getString(rootNode, PROP_PROXY_HOST_NAME));
    slaveServer.setProxyPort(getString(rootNode, PROP_PROXY_PORT));
    slaveServer.setNonProxyHosts(getString(rootNode, PROP_NON_PROXY_HOSTS));
    slaveServer.setMaster(rootNode.getProperty(PROP_MASTER).getBoolean());

  }

  public DataNode elementToDataNode(RepositoryElementInterface element) throws KettleException {
    SlaveServer slaveServer = (SlaveServer) element;
    DataNode rootNode = new DataNode(NODE_ROOT);
    PentahoDscContent dscContent = PentahoLicenseVerifier.verify(new KParam());

    // Check for naming collision
    ObjectId slaveId = repo.getSlaveID(slaveServer.getName());
    if (slaveId != null && !slaveServer.getObjectId().equals(slaveId)) {
      // We have a naming collision, abort the save
      throw new KettleException("Failed to save object to repository. Object [" + slaveServer.getName()
          + "] already exists.");
    }

    // Create or version a new slave node
    //
    if (dscContent.getSubject() != null) {
      rootNode.setProperty(PROP_NAME, slaveServer.getName());
      rootNode.setProperty(PROP_DESCRIPTION, slaveServer.getDescription());
      rootNode.setProperty(PROP_HOST_NAME, slaveServer.getHostname());
      rootNode.setProperty(PROP_PORT, slaveServer.getPort());
      rootNode.setProperty(PROP_USERNAME, slaveServer.getUsername());
      rootNode.setProperty(PROP_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(slaveServer.getPassword()));
      rootNode.setProperty(PROP_PROXY_HOST_NAME, slaveServer.getProxyHostname());
      rootNode.setProperty(PROP_PROXY_PORT, slaveServer.getProxyPort());
      rootNode.setProperty(PROP_NON_PROXY_HOSTS, slaveServer.getNonProxyHosts());
      rootNode.setProperty(PROP_MASTER, slaveServer.isMaster());
    }
    return rootNode;
  }

}
