package org.pentaho.di.repository.pur;

public class RepositoryObjectAccessException extends Exception {

  public enum AccessExceptionType {
    USER_HOME_DIR
  }
  
  private static final long serialVersionUID = 1L;
  private AccessExceptionType type;
  
  public RepositoryObjectAccessException(String message, AccessExceptionType type) {
    this.type = type;
  }
  
  public AccessExceptionType getObjectAccessType() {
    return type;
  }
  
  public void setObjectAccessType(AccessExceptionType type) {
    this.type = type;
  }

}
