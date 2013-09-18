/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.ui.repository.pur.repositoryexplorer.model;

import java.util.List;

import org.pentaho.ui.xul.util.AbstractModelNode;

public class UIRepositoryObjectRevisions extends AbstractModelNode<UIRepositoryObjectRevision> implements java.io.Serializable {

  private static final long serialVersionUID = -5243106180726024567L; /* EESOURCE: UPDATE SERIALVERUID */

    
  public UIRepositoryObjectRevisions(){
  }
  
  public UIRepositoryObjectRevisions(List<UIRepositoryObjectRevision> revisions){
    super(revisions);
  }
  
  @Override
  protected void fireCollectionChanged() {
    this.changeSupport.firePropertyChange("children", null, this);
  }
  
}
