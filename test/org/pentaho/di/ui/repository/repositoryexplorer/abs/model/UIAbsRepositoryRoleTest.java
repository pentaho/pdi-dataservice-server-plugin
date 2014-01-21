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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.repository.pur.model.AbsRoleInfo;
import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.model.UIAbsRepositoryRole;

public class UIAbsRepositoryRoleTest implements java.io.Serializable {

  static final long serialVersionUID = -3922751737899149745L; /* EESOURCE: UPDATE SERIALVERUID */

  public final static String CREATE_CONTENT = "org.pentaho.di.creator"; //$NON-NLS-1$
  public final static String READ_CONTENT = "org.pentaho.di.reader";//$NON-NLS-1$
  public final static String ADMINISTER_SECURITY = "org.pentaho.di.securityAdministrator";//$NON-NLS-1$

  @Before
  public void init() {   
    
  }
  @Test
  public void testUIAbsRepositoryRole()  throws Exception {
    IRole role = new AbsRoleInfo();
    role.setDescription("role description");
    role.setName("myrole");
    UIAbsRepositoryRole uiRole = new UIAbsRepositoryRole(role);
    Assert.assertEquals(uiRole.getLogicalRoles().size(), 0);
    uiRole.addLogicalRole(CREATE_CONTENT);
    Assert.assertEquals(uiRole.getLogicalRoles().size(), 1);
    uiRole.removeLogicalRole(CREATE_CONTENT);
    Assert.assertEquals(uiRole.getLogicalRoles().size(), 0);
    List<String> logicalRoles = new ArrayList<String>();
    logicalRoles.add(CREATE_CONTENT);
    logicalRoles.add(READ_CONTENT);
    logicalRoles.add(ADMINISTER_SECURITY);
    uiRole.setLogicalRoles(logicalRoles);
    Assert.assertEquals(uiRole.getLogicalRoles().size(), 3);
  }
}
