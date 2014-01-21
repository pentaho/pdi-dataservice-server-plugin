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

package org.pentaho.di.ui.repository.repositoryexplorer.abs.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.UIEEObjectRegistery;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model.UIAbsRepositoryRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model.UIAbsSecurity;
import org.pentaho.di.ui.repository.pur.services.IRoleSupportSecurityManager;

public class UIAbsSecurityTest implements java.io.Serializable {

  static final long serialVersionUID = -8052299792314313987L; /* EESOURCE: UPDATE SERIALVERUID */

  IRoleSupportSecurityManager sm; 
  public final static String CREATE_CONTENT = "org.pentaho.di.creator"; //$NON-NLS-1$
  public final static String READ_CONTENT = "org.pentaho.di.reader";//$NON-NLS-1$
  public final static String ADMINISTER_SECURITY = "org.pentaho.di.securityAdministrator";//$NON-NLS-1$

  @Before
  public void init() {   
    sm = new RepsitoryUserTestImpl();
  }
  @Test
  public void testUIAbsSecurity()  throws Exception {
    UIEEObjectRegistery.getInstance().registerUIRepositoryRoleClass(UIAbsRepositoryRole.class);
    UIAbsSecurity security = new UIAbsSecurity(sm);
    security.setSelectedRole(new UIAbsRepositoryRole(sm.getRoles().get(0)));
    Assert.assertEquals(((UIAbsRepositoryRole) security.getSelectedRole()).getLogicalRoles().size(), 0);
    security.addLogicalRole(CREATE_CONTENT);
    Assert.assertEquals(((UIAbsRepositoryRole) security.getSelectedRole()).getLogicalRoles().size(), 1);
    security.removeLogicalRole(CREATE_CONTENT);
    Assert.assertEquals(((UIAbsRepositoryRole) security.getSelectedRole()).getLogicalRoles().size(), 0);    
  }
}
