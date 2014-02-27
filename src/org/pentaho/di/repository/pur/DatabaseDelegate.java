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

import java.util.Enumeration;
import java.util.Properties;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.repository2.unified.VersionSummary;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;
import org.pentaho.platform.api.repository2.unified.data.node.DataProperty;
import org.pentaho.platform.api.repository2.unified.data.node.NodeRepositoryFileData;
import org.pentaho.platform.repository.RepositoryFilenameUtils;

public class DatabaseDelegate extends AbstractDelegate implements ITransformer, SharedObjectAssembler<DatabaseMeta>, java.io.Serializable {

  private static final long serialVersionUID = 1512547938350522165L; /* EESOURCE: UPDATE SERIALVERUID */

  // ~ Static fields/initializers ======================================================================================

  private static final String PROP_INDEX_TBS = "INDEX_TBS"; //$NON-NLS-1$

  private static final String PROP_DATA_TBS = "DATA_TBS"; //$NON-NLS-1$

  private static final String PROP_SERVERNAME = "SERVERNAME"; //$NON-NLS-1$

  private static final String PROP_PASSWORD = "PASSWORD"; //$NON-NLS-1$

  private static final String PROP_USERNAME = "USERNAME"; //$NON-NLS-1$

  private static final String PROP_PORT = "PORT"; //$NON-NLS-1$

  private static final String PROP_DATABASE_NAME = "DATABASE_NAME"; //$NON-NLS-1$

  private static final String PROP_HOST_NAME = "HOST_NAME"; //$NON-NLS-1$

  private static final String PROP_CONTYPE = "CONTYPE"; //$NON-NLS-1$

  private static final String PROP_TYPE = "TYPE"; //$NON-NLS-1$

  private static final String NODE_ROOT = "databaseMeta"; //$NON-NLS-1$

  private static final String NODE_ATTRIBUTES = "attributes"; //$NON-NLS-1$

  // ~ Instance fields =================================================================================================
  
  private PurRepository repo;
  
  // ~ Constructors ====================================================================================================

  public DatabaseDelegate(final PurRepository repo) {
    super();
    this.repo = repo;
  }

  // ~ Methods =========================================================================================================

  public DataNode elementToDataNode(final RepositoryElementInterface element) throws KettleException {
    DatabaseMeta databaseMeta = (DatabaseMeta) element;
    DataNode rootNode = new DataNode(NODE_ROOT);

    // Then the basic db information
    //
    rootNode.setProperty(PROP_TYPE, databaseMeta.getPluginId());
    rootNode.setProperty(PROP_CONTYPE, DatabaseMeta.getAccessTypeDesc(databaseMeta.getAccessType()));
    rootNode.setProperty(PROP_HOST_NAME, databaseMeta.getHostname());
    rootNode.setProperty(PROP_DATABASE_NAME, databaseMeta.getDatabaseName());
    rootNode.setProperty(PROP_PORT, new Long(Const.toInt(databaseMeta.getDatabasePortNumberString(), -1)));
    rootNode.setProperty(PROP_USERNAME, databaseMeta.getUsername());
    rootNode.setProperty(PROP_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(databaseMeta.getPassword()));
    rootNode.setProperty(PROP_SERVERNAME, databaseMeta.getServername());
    rootNode.setProperty(PROP_DATA_TBS, databaseMeta.getDataTablespace());
    rootNode.setProperty(PROP_INDEX_TBS, databaseMeta.getIndexTablespace());

    DataNode attrNode = rootNode.addNode(NODE_ATTRIBUTES);

	// Now store all the attributes set on the database connection...
	// 
	Properties attributes = databaseMeta.getAttributes();
	Enumeration<Object> keys = databaseMeta.getAttributes().keys();
	while (keys.hasMoreElements()) {
	  String code = (String) keys.nextElement();
	  String attribute = (String) attributes.get(code);
	
	  // Save this attribute
	  //
    // Escape the code as it might contain invalid JCR characters like '/' as in AS/400
    String escapedCode = RepositoryFilenameUtils.escape(code, repo.getPur().getReservedChars());
    attrNode.setProperty(escapedCode, attribute);
	}
    return rootNode;

  }

  public RepositoryElementInterface dataNodeToElement(final DataNode rootNode) throws KettleException {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    dataNodeToElement(rootNode, databaseMeta);
    return databaseMeta;
  }
  
  public void dataNodeToElement(final DataNode rootNode, final RepositoryElementInterface element) throws KettleException {
    DatabaseMeta databaseMeta = (DatabaseMeta) element;
    databaseMeta.setDatabaseType(getString(rootNode, PROP_TYPE));
    databaseMeta.setAccessType(DatabaseMeta.getAccessType(getString(rootNode, PROP_CONTYPE)));
    databaseMeta.setHostname(getString(rootNode, PROP_HOST_NAME));
    databaseMeta.setDBName(getString(rootNode, PROP_DATABASE_NAME));
    databaseMeta.setDBPort(getString(rootNode, PROP_PORT));
    databaseMeta.setUsername(getString(rootNode, PROP_USERNAME));
    databaseMeta.setPassword(Encr.decryptPasswordOptionallyEncrypted(getString(rootNode, PROP_PASSWORD)));
    databaseMeta.setServername(getString(rootNode, PROP_SERVERNAME));
    databaseMeta.setDataTablespace(getString(rootNode, PROP_DATA_TBS));
    databaseMeta.setIndexTablespace(getString(rootNode, PROP_INDEX_TBS));

    // Also, load all the properties we can find...

    DataNode attrNode = rootNode.getNode(NODE_ATTRIBUTES);
    for (DataProperty property : attrNode.getProperties()) {
      String code = property.getName();
      String attribute = property.getString();

      // We need to unescape the code as it was escaped to handle characters that JCR does not handle
      String unescapeCode = RepositoryFilenameUtils.unescape(code);
      databaseMeta.getAttributes().put(unescapeCode, Const.NVL(attribute, "")); //$NON-NLS-1$
    }
  }

  public Repository getRepository() {
    return repo;
  }

  public DatabaseMeta assemble(RepositoryFile file, NodeRepositoryFileData data, VersionSummary version) throws KettleException {
    DatabaseMeta databaseMeta = (DatabaseMeta) dataNodeToElement(data.getNode());
    String fileName = file.getName();
    if (fileName.endsWith(".kdb")) {
      fileName = fileName.substring(0, fileName.length() - 4);
    }
    databaseMeta.setName(fileName);
    databaseMeta.setDisplayName( file.getTitle() );
    databaseMeta.setObjectId(new StringObjectId(file.getId().toString()));
    databaseMeta.setObjectRevision(repo.createObjectRevision(version));
    databaseMeta.clearChanged();
    return databaseMeta;
  }
}
