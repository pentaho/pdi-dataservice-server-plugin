/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.platform.api.repository2.unified.data.node.DataNode;

public interface ITransformer {

  DataNode elementToDataNode(final RepositoryElementInterface element) throws KettleException;

  RepositoryElementInterface dataNodeToElement(final DataNode rootNode) throws KettleException;
  
  void dataNodeToElement(final DataNode rootNode, final RepositoryElementInterface element) throws KettleException;

}
