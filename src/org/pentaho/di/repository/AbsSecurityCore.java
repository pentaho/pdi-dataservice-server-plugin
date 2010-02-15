package org.pentaho.di.repository;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.repository.pur.PurRepository;
import org.pentaho.di.repository.pur.PurRepositoryMeta;
import org.pentaho.di.repository.pur.PurRepositorySecurityProvider;

public class AbsSecurityCore extends PurRepositorySecurityProvider implements IAbsCore{

  public AbsSecurityCore(PurRepository repository, PurRepositoryMeta repositoryMeta, UserInfo userInfo) {
    super(repository, repositoryMeta, userInfo);
    // TODO Auto-generated constructor stub
  }

  public List<String> getAllowedActions() {
    // TODO Auto-generated method stub
    List<String> allowedAction = new ArrayList<String>();
    allowedAction.add("Create Content");
    allowedAction.add("Read Content");
    allowedAction.add("Administer Security");
    return allowedAction;
  }

  public boolean isAllowed(String action) {
    // TODO Auto-generated method stub
    return true;
  }

}
