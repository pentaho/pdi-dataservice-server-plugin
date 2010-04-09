package org.pentaho.di.ui.repository.pur.repositoryexplorer;

import java.util.Set;

import org.pentaho.di.repository.pur.model.IRole;
import org.pentaho.di.ui.repository.repositoryexplorer.model.IUIUser;

public interface IUIRole {

  public void setName(String name);
  public String getName();
  public String getDescription();
  public void setDescription(String description);
  public void setUsers(Set<IUIUser> users);
  public Set<IUIUser> getUsers();
  public boolean addUser(IUIUser user);
  public boolean removeUser(IUIUser user);
  public void clearUsers();
  public IRole getRole();
}
