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

package org.pentaho.di.ui.repository.pur.repositoryexplorer;

import java.lang.reflect.Constructor;

import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.model.UIRepositoryRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIObjectCreationException;

public class UIEEObjectRegistery implements java.io.Serializable {

  private static final long serialVersionUID = 1405941020109398651L; /* EESOURCE: UPDATE SERIALVERUID */

  public static final Class<?> DEFAULT_UIREPOSITORYROLE_CLASS = UIRepositoryRole.class;
  
  private static UIEEObjectRegistery instance;

  private Class<?> repositoryRoleClass;
  
  private UIEEObjectRegistery() {

  }

  public static UIEEObjectRegistery getInstance() {
    if (instance == null) {
      instance = new UIEEObjectRegistery();
    }
    return instance;
  }

  public void registerUIRepositoryRoleClass(Class<?> repositoryRoleClass) {
    this.repositoryRoleClass = repositoryRoleClass;
  }

  public Class<?> getRegisteredUIRepositoryRoleClass() {
    return this.repositoryRoleClass;
  }

  public IUIRole constructUIRepositoryRole(IRole role) throws UIObjectCreationException {
    try {
      if(repositoryRoleClass == null) {
        repositoryRoleClass = DEFAULT_UIREPOSITORYROLE_CLASS;
      }
      Constructor<?> constructor = repositoryRoleClass.getConstructor(IRole.class);
      if (constructor != null) {
        return (IUIRole) constructor.newInstance(role);
      } else {
        throw new UIObjectCreationException("Unable to get the constructor for " + repositoryRoleClass);
      }
    } catch (Exception e) {
      throw new UIObjectCreationException("Unable to instantiate object for " + repositoryRoleClass);
    }
  }
}
