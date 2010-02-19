package org.pentaho.di.repository;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.Service;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.PurRepositoryMeta;
import org.pentaho.di.ui.repository.repositoryexplorer.abs.AbsSpoonPlugin;

import com.pentaho.security.policy.rolebased.RoleBindingStruct;
import com.pentaho.security.policy.rolebased.ws.IRoleAuthorizationPolicyRoleBindingDaoWebService;

public class AbsSecurityManager extends AbsSecurityProvider implements IAbsSecurityManager {

  public final static String CREATE_CONTENT_ROLE = "org.pentaho.di.creator"; //$NON-NLS-1$

  public final static String READ_CONTENT_ROLE = "org.pentaho.di.reader";//$NON-NLS-1$

  public final static String ADMINISTER_SECURITY_ROLE = "org.pentaho.di.securityAdministrator";//$NON-NLS-1$

  public final static String CREATE_CONTENT_ACTION = "org.pentaho.repository.create"; //$NON-NLS-1$

  public final static String READ_CONTENT_ACTION = "org.pentaho.repository.read";//$NON-NLS-1$

  public final static String ADMINISTER_SECURITY_ACTION = "org.pentaho.security.administerSecurity";//$NON-NLS-1$

  public final static String NAMESPACE = "org.pentaho"; //$NON-NLS-1$

  private IRoleAuthorizationPolicyRoleBindingDaoWebService authorizationPolicyRoleBindingService = null;

  private RoleBindingStruct roleBindingStruct = null;

  public AbsSecurityManager(Repository repository, RepositoryMeta repositoryMeta, UserInfo userInfo) {
    super((PurRepository) repository, (PurRepositoryMeta) repositoryMeta, userInfo);
    try {
      if (allowedActionsContains(ADMINISTER_SECURITY_ACTION)) {
        final String url = ((PurRepositoryMeta) repositoryMeta).getRepositoryLocation().getUrl()
            + "/roleBindingDao?wsdl"; //$NON-NLS-1$
        Service service = Service.create(new URL(url), new QName("http://www.pentaho.org/ws/1.0", //$NON-NLS-1$
            "DefaultRoleAuthorizationPolicyRoleBindingDaoWebServiceService"));//$NON-NLS-1$
        authorizationPolicyRoleBindingService = service.getPort(IRoleAuthorizationPolicyRoleBindingDaoWebService.class);
        ((BindingProvider) authorizationPolicyRoleBindingService).getRequestContext().put(
            BindingProvider.USERNAME_PROPERTY, userInfo.getLogin());
        ((BindingProvider) authorizationPolicyRoleBindingService).getRequestContext().put(
            BindingProvider.PASSWORD_PROPERTY, userInfo.getPassword());
        if (authorizationPolicyRoleBindingService == null) {
          getLogger().error(
              BaseMessages.getString(AbsSpoonPlugin.class,
                  "AbsSecurityManager.ERROR_0001_UNABLE_TO_INITIALIZE_ROLE_BINDING_WEBSVC")); //$NON-NLS-1$
        }
      }
    } catch (Exception e) {
      getLogger().error(
          BaseMessages.getString(AbsSpoonPlugin.class,
              "AbsSecurityManager.ERROR_0001_UNABLE_TO_INITIALIZE_ROLE_BINDING_WEBSVC"), e); //$NON-NLS-1$
    }
  }

  private boolean allowedActionsContains(String action) throws KettleException {
    List<String> allowedActions = getAllowedActions(NAMESPACE);
    for (String actionName : allowedActions) {
      if (action != null && action.equals(actionName)) {
        return true;
      }
    }
    return false;
  }

  public void initialize(String locale) throws KettleException {
    if (authorizationPolicyRoleBindingService != null) {
      try {
        roleBindingStruct = authorizationPolicyRoleBindingService.getRoleBindingStruct(locale);
      } catch (Exception e) {
        throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
            "AbsSecurityManager.ERROR_0002_UNABLE_TO_GET_LOGICAL_ROLES"), e); //$NON-NLS-1$
      }
    } else {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0005_INSUFFICIENT_PRIVELEGES", getUserInfo().getLogin())); //$NON-NLS-1$
    }
  }

  @Override
  public IRole getRole(String name) throws KettleException {
    IRole role = super.getRole(name);
    if (role instanceof ILogicalRole) {
      List<String> logicalRoles = getLogicalRoles(role.getName());
      if (logicalRoles != null && logicalRoles.size() > 0) {
        ((ILogicalRole) role).setLogicalRoles(logicalRoles);
      }
    }
    return role;
  }

  @Override
  public List<IRole> getRoles() throws KettleException {
    List<IRole> roles = super.getRoles();
    for (IRole role : roles) {
      if (role instanceof ILogicalRole) {
        List<String> logicalRoles = getLogicalRoles(role.getName());
        if (logicalRoles != null && logicalRoles.size() > 0) {
          ((ILogicalRole) role).setLogicalRoles(logicalRoles);
        }
      }
    }
    return roles;
  }

  public List<String> getLocalizedLogicalRoles(String runtimeRole, String locale) throws KettleException {
    if (authorizationPolicyRoleBindingService != null) {
      List<String> localizedLogicalRoles = new ArrayList<String>();
      if (roleBindingStruct != null && roleBindingStruct.logicalRoleNameMap != null) {
        List<String> logicalRoles = getLogicalRoles(runtimeRole);
        for (String logicalRole : logicalRoles) {
          localizedLogicalRoles.add(roleBindingStruct.logicalRoleNameMap.get(logicalRole));
        }
      } else {
        throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
            "AbsSecurityManager.ERROR_0003_UNABLE_TO_ACCESS_ROLE_BINDING_WEBSVC")); //$NON-NLS-1$
      }
      return localizedLogicalRoles;
    } else {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0005_INSUFFICIENT_PRIVELEGES", getUserInfo().getLogin())); //$NON-NLS-1$
    }
  }

  public List<String> getLogicalRoles(String runtimeRole) throws KettleException {
    if (authorizationPolicyRoleBindingService != null) {
      if (roleBindingStruct != null && roleBindingStruct.bindingMap != null
          && roleBindingStruct.bindingMap.containsKey(runtimeRole)) {
        return roleBindingStruct.bindingMap.get(runtimeRole);
      }
      return null;
    } else {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0005_INSUFFICIENT_PRIVELEGES", getUserInfo().getLogin())); //$NON-NLS-1$
    }
  }

  public void setLogicalRoles(String rolename, List<String> logicalRoles) throws KettleException {
    if(authorizationPolicyRoleBindingService != null) {    
    try {
      authorizationPolicyRoleBindingService.setRoleBindings(rolename, logicalRoles);
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0004_UNABLE_TO_APPLY_LOGICAL_ROLES_TO_RUNTIME_ROLE"), e); //$NON-NLS-1$
    }
    } else {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0005_INSUFFICIENT_PRIVELEGES", getUserInfo().getLogin())); //$NON-NLS-1$
    }
  }

  public Map<String, String> getAllLogicalRoles(String locale) throws KettleException {
    if(authorizationPolicyRoleBindingService != null) {    
      return roleBindingStruct.logicalRoleNameMap;
    } else {
      throw new KettleException(BaseMessages.getString(AbsSpoonPlugin.class,
          "AbsSecurityManager.ERROR_0005_INSUFFICIENT_PRIVELEGES", getUserInfo().getLogin())); //$NON-NLS-1$
    }
  }

}
