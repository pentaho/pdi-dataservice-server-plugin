package org.pentaho.di.repository.pur;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.shared.SharedObjects;

public interface ISharedObjectsHelper extends ITransformer {
  void loadSharedObjects(final RepositoryElementInterface element) throws KettleException;
  
  void saveSharedObjects(final RepositoryElementInterface element, final String versionComment) throws KettleException;
}
