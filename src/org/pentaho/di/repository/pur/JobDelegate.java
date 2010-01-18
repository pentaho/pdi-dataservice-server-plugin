package org.pentaho.di.repository.pur;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.platform.repository.pcr.data.node.DataNode;

public class JobDelegate extends AbstractDelegate implements ISharedObjectsTransformer {

  // ~ Static fields/initializers ======================================================================================

  // ~ Instance fields =================================================================================================

  private Repository repo;

  // ~ Constructors ====================================================================================================

  public JobDelegate(final Repository repo) {
    super();
    this.repo = repo;
  }

  // ~ Methods =========================================================================================================

  public void loadSharedObjects(final RepositoryElementInterface element) throws KettleException {

    // TODO Auto-generated method stub 

  }

  public void saveSharedObjects(final RepositoryElementInterface element, final String versionComment)
      throws KettleException {

    // TODO Auto-generated method stub 

  }

  public RepositoryElementInterface dataNodeToElement(final DataNode rootNode) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

  public void dataNodeToElement(final DataNode rootNode, final RepositoryElementInterface element)
      throws KettleException {

    // TODO Auto-generated method stub 

  }

  public DataNode elementToDataNode(final RepositoryElementInterface element) throws KettleException {

    // TODO Auto-generated method stub 
    return null;

  }

}
