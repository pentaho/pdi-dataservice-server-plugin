package org.pentaho.di.ui.repository.repositoryexplorer.abs.controller;

import org.pentaho.ui.xul.XulEventSourceAdapter;

public class PermissionPropertyModel extends XulEventSourceAdapter{
  private static final String createPermissionProperty = "createPermissionGranted"; //$NON-NLS-1$
  private static final String readPermissionProperty = "readPermissionGranted"; //$NON-NLS-1$
  
  private boolean createPermissionGranted = false;

  private boolean readPermissionGranted = false;
  

  public void setCreatePermissionGranted(boolean createPermissionGranted) {
    this.createPermissionGranted = createPermissionGranted;
    this.firePropertyChange(createPermissionProperty, null, createPermissionGranted);
  }

  public boolean isCreatePermissionGranted() {
    return createPermissionGranted;
  }

  public void setReadPermissionGranted(boolean readPermissionGranted) {
    this.readPermissionGranted = readPermissionGranted;
    this.firePropertyChange(readPermissionProperty, null, readPermissionGranted);
  }

  public boolean isReadPermissionGranted() {
    return readPermissionGranted;
  }
}
