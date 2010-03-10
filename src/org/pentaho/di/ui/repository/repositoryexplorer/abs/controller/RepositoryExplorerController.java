/*
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2009 Pentaho Corporation.  All rights reserved.
 */

package org.pentaho.di.ui.repository.repositoryexplorer.abs.controller;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.repository.repositoryexplorer.RepositoryExplorer;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.ClustersController;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.ConnectionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.PartitionsController;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.SlavesController;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class RepositoryExplorerController extends AbstractXulEventHandler {

  private static final String createPermissionProperty = "createPermissionGranted"; //$NON-NLS-1$
  private static final String readPermissionProperty = "readPermissionGranted"; //$NON-NLS-1$
  
  private boolean createPermissionGranted = false;

  private boolean readPermissionGranted = false;
  
  private static final Class<?> CLZ = RepositoryExplorer.class;
  ConnectionsControllerOverride connCon;
  PartitionsController partCon;
  SlavesController slaveCon;
  ClustersController clustCon;
  private ResourceBundle resourceBundle = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(CLZ, key);
    }
    
  }; 

  @Override
  public String getName() {
    return "ABSHandler"; //$NON-NLS-1$
  }

  public class ConnectionsControllerOverride extends ConnectionsController {
    @Override
    public void setEnableButtons(boolean enable) {
      if(RepositoryExplorerController.this.isCreatePermissionGranted()) {
        enableButtons(true, enable, enable);
      } else {
        enableButtons(false, false, false);
      }
    }
  };
  
  public class SlavesControllerOverride extends SlavesController {
    @Override
    public void setEnableButtons(boolean enable) {
      if(RepositoryExplorerController.this.isCreatePermissionGranted()) {
        enableButtons(true, enable, enable);
      } else {
        enableButtons(false, false, false);
      }
    }
  };
  
  public class PartitionsControllerOverride extends PartitionsController {
    @Override
    public void setEnableButtons(boolean enable) {
      if(RepositoryExplorerController.this.isCreatePermissionGranted()) {
        enableButtons(true, enable, enable);
      } else {
        enableButtons(false, false, false);
      }
    }
  };
  
  public class ClustersControllerOverride extends ClustersController {
    @Override
    public void setEnableButtons(boolean enable) {
      if(RepositoryExplorerController.this.isCreatePermissionGranted()) {
        enableButtons(true, enable, enable);
      } else {
        enableButtons(false, false, false);
      }
    }
  };

  public void setRepository(Repository rep) {
/*    connCon.setRepository(rep);
    slaveCon.setRepository(rep);
    partCon.setRepository(rep);
    clustCon.setRepository(rep);*/
  }
  public void init() {
    // Create bindings
    Document doc = getXulDomContainer().getDocumentRoot();
    
    BindingFactory bf = new DefaultBindingFactory();
    bf.setDocument(doc);
    bf.setBindingType(Binding.Type.ONE_WAY);
    
    // Override repository explorer handlers to check permissions
    
    connCon = new ConnectionsControllerOverride();
    
    getXulDomContainer().addEventHandler(connCon);
    
    slaveCon = new SlavesControllerOverride();
    
    getXulDomContainer().addEventHandler(slaveCon);
    
    partCon = new PartitionsControllerOverride();
    
    partCon.setVariableSpace(Variables.getADefaultVariableSpace());
    
    getXulDomContainer().addEventHandler(partCon);
    
    clustCon = new ClustersControllerOverride();
    getXulDomContainer().addEventHandler(clustCon);
    
    // Generate "Create" permissions bindings
    bf.createBinding(this, createPermissionProperty, "file-context-rename", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "file-context-delete", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "folder-context-create", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "folder-context-rename", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "folder-context-delete", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$

    bf.createBinding(this, createPermissionProperty, "connections-edit", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "connections-new", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "connections-remove", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    
    bf.createBinding(this, createPermissionProperty, "slaves-edit", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "slaves-new", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "slaves-remove", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    
    bf.createBinding(this, createPermissionProperty, "partitions-edit", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "partitions-new", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "partitions-remove", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    
    bf.createBinding(this, createPermissionProperty, "clusters-edit", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "clusters-new", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding(this, createPermissionProperty, "clusters-remove", "!disabled"); //$NON-NLS-1$ //$NON-NLS-2$
    
    // Set current UI state
    firePropertyChange(createPermissionProperty, null, createPermissionGranted);
  }

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
