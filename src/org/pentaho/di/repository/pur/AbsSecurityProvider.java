/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.platform.security.policy.rolebased.ws.IAuthorizationPolicyWebService;

public class AbsSecurityProvider extends PurRepositorySecurityProvider implements IAbsSecurityProvider, java.io.Serializable {

  private static final long serialVersionUID = -41954375242408881L; /* EESOURCE: UPDATE SERIALVERUID */
  private IAuthorizationPolicyWebService authorizationPolicyWebService = null;

  public AbsSecurityProvider( PurRepository repository, PurRepositoryMeta repositoryMeta, IUser userInfo,
      ServiceManager serviceManager ) {
    super( repository, repositoryMeta, userInfo, serviceManager );
    try {
      authorizationPolicyWebService =
          serviceManager.createService( userInfo.getLogin(), userInfo.getPassword(),
              IAuthorizationPolicyWebService.class );
      if ( authorizationPolicyWebService == null ) {
        getLogger().error(
            BaseMessages.getString( AbsSecurityProvider.class,
                "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC" ) ); //$NON-NLS-1$
      }

    } catch ( Exception e ) {
      getLogger().error(
          BaseMessages.getString( AbsSecurityProvider.class,
              "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC" ), e ); //$NON-NLS-1$
    }
  }

  public List<String> getAllowedActions(String nameSpace) throws KettleException {
    try {
      return authorizationPolicyWebService.getAllowedActions(nameSpace);
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(AbsSecurityProvider.class,
          "AbsSecurityProvider.ERROR_0003_UNABLE_TO_ACCESS_GET_ALLOWED_ACTIONS"), e); //$NON-NLS-1$
    }
  }

  public boolean isAllowed(String actionName) throws KettleException {
    try {
      return authorizationPolicyWebService.isAllowed(actionName);
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(AbsSecurityProvider.class,
          "AbsSecurityProvider.ERROR_0002_UNABLE_TO_ACCESS_IS_ALLOWED"), e);//$NON-NLS-1$
    }
  }
}
