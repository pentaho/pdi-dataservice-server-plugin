/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;

public interface ISharedObjectsTransformer extends ITransformer {
  SharedObjects loadSharedObjects(final RepositoryElementInterface element,
      final Map<RepositoryObjectType, List<? extends SharedObjectInterface>> sharedObjectsByType)
      throws KettleException;
  
  void saveSharedObjects(final RepositoryElementInterface element, final String versionComment) throws KettleException;
}
