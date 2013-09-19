/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

public class RepositoryObjectAccessException extends Exception implements java.io.Serializable {

  private static final long serialVersionUID = -3339087102211752867L; /* EESOURCE: UPDATE SERIALVERUID */

  public enum AccessExceptionType {
    USER_HOME_DIR
  }
  
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
