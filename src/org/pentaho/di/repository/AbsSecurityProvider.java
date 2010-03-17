package org.pentaho.di.repository;

import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.PurRepositoryMeta;
import org.pentaho.di.repository.pur.PurRepositorySecurityProvider;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSpoonPlugin;

import com.pentaho.security.policy.rolebased.ws.IAuthorizationPolicyWebService;

public class AbsSecurityProvider extends PurRepositorySecurityProvider implements IAbsSecurityProvider{
  private IAuthorizationPolicyWebService authorizationPolicyWebService = null;

  public AbsSecurityProvider(PurRepository repository, PurRepositoryMeta repositoryMeta, IUser userInfo) {
    super(repository, repositoryMeta, userInfo);
    try {
      final String url = repositoryMeta.getRepositoryLocation().getUrl() + "/webservices/authorizationPolicy?wsdl"; //$NON-NLS-1$
      Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", //$NON-NLS-1$
          "authorizationPolicy"));//$NON-NLS-1$
      authorizationPolicyWebService = service.getPort(IAuthorizationPolicyWebService.class);
      ((BindingProvider) authorizationPolicyWebService).getRequestContext().put(BindingProvider.USERNAME_PROPERTY,
          userInfo.getLogin());
      ((BindingProvider) authorizationPolicyWebService).getRequestContext().put(BindingProvider.PASSWORD_PROPERTY,
          userInfo.getPassword());
      if (authorizationPolicyWebService == null) {
        getLogger().error(
            BaseMessages.getString(AbsSpoonPlugin.class,
                "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC")); //$NON-NLS-1$
      }

    } catch (Exception e) {
      getLogger().error(
          BaseMessages.getString(AbsSpoonPlugin.class,
              "AbsSecurityProvider.ERROR_0001_UNABLE_TO_INITIALIZE_AUTH_POLICY_WEBSVC"), e); //$NON-NLS-1$
    }
  }

  public List<String> getAllowedActions(String nameSpace) throws KettleException {
    try {
      return authorizationPolicyWebService.getAllowedActions(nameSpace);
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityProvider.ERROR_0003_UNABLE_TO_ACCESS_GET_ALLOWED_ACTIONS"), e); //$NON-NLS-1$
    }
  }

  public boolean isAllowed(String actionName) throws KettleException {
    try {
      return authorizationPolicyWebService.isAllowed(actionName);
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityProvider.ERROR_0002_UNABLE_TO_ACCESS_IS_ALLOWED"), e);//$NON-NLS-1$
    }
  }
}
