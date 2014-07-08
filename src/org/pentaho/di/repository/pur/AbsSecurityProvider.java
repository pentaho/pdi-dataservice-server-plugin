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

package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.RepositoryOperation;
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

  @Override
  public void validateAction( RepositoryOperation... operations )
      throws KettleException, KettleSecurityException {

    for ( RepositoryOperation operation : operations ) {
      if ( ( operation == RepositoryOperation.EXECUTE_TRANSFORMATION ) ||
           ( operation == RepositoryOperation.EXECUTE_JOB) ) {
        if ( isAllowed( IAbsSecurityProvider.EXECUTE_CONTENT_ACTION ) == false ) {
          throw new KettleException( operation + " : permission not allowed" );
        }
      } else if ( ( operation == RepositoryOperation.MODIFY_TRANSFORMATION ) ||
                  ( operation == RepositoryOperation.MODIFY_JOB ) ) {
        if ( isAllowed( IAbsSecurityProvider.CREATE_CONTENT_ACTION ) == false ) {
          throw new KettleException( operation + " : permission not allowed" );
        }
      }
    }
  }
}
