package org.pentaho.di.repository;

import java.util.List;

public interface IAbsCore {
  public boolean isAllowed(String action);
  public List<String> getAllowedActions();
}
