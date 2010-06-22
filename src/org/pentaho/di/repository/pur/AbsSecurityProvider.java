package org.pentaho.di.repository.pur;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.ui.repository.pur.services.IAbsSecurityProvider;
import org.pentaho.platform.security.policy.rolebased.ws.IAuthorizationPolicyWebService;

public class AbsSecurityProvider extends PurRepositorySecurityProvider implements IAbsSecurityProvider{
  private IAuthorizationPolicyWebService authorizationPolicyWebService = null;

  public AbsSecurityProvider(PurRepository repository, PurRepositoryMeta repositoryMeta, IUser userInfo) {
    super(repository, repositoryMeta, userInfo);
    try {
      authorizationPolicyWebService = WsFactory.createService(repositoryMeta, "authorizationPolicy", userInfo //$NON-NLS-1$
          .getLogin(), userInfo.getPassword(), IAuthorizationPolicyWebService.class);
      if (authorizationPolicyWebService == null) {
        getLogger().error(
            BaseMessages.getString(AbsSecurityProvider.class,
                "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC")); //$NON-NLS-1$
      }

    } catch (Exception e) {
      getLogger().error(
          BaseMessages.getString(AbsSecurityProvider.class,
              "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC"), e); //$NON-NLS-1$
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
