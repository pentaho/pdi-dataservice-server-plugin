package org.pentaho.di.repository.pur;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.platform.repository2.unified.data.node.DataNode;

public interface ITransformer {

  DataNode elementToDataNode(final RepositoryElementInterface element) throws KettleException;

  RepositoryElementInterface dataNodeToElement(final DataNode rootNode) throws KettleException;
  
  void dataNodeToElement(final DataNode rootNode, final RepositoryElementInterface element) throws KettleException;

}
