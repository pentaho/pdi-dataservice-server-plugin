/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.ui.repository.pur.repositoryexplorer;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectRevision;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryObjectRevisions;

public interface IRevisionObject {
  public UIRepositoryObjectRevisions getRevisions() throws KettleException;
  public void restoreRevision(UIRepositoryObjectRevision revision, String commitMessage) throws KettleException;
}
