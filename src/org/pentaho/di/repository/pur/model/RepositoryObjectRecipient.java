/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur.model;

import org.pentaho.di.repository.ObjectRecipient;


public class RepositoryObjectRecipient implements ObjectRecipient, java.io.Serializable {

  private static final long serialVersionUID = 3948870815049027653L; /* EESOURCE: UPDATE SERIALVERUID */

	@Override
  public boolean equals(Object obj) {
	  if(obj != null) {
  	  RepositoryObjectRecipient recipient = (RepositoryObjectRecipient) obj;
  	  if(name == null && type == null && recipient.getName() == null && recipient.getType() == null) {
  	    return true;
  	  } else if(name != null && type != null) {
  	    return name.equals(recipient.getName()) && type.equals(recipient.getType());	    
  	  } else if (recipient.getName() != null && recipient.getType() != null ){
        return recipient.getName().equals(name) && recipient.getType().equals(type);
      } else if (recipient.getType() == null && type == null) {
        return name.equals(recipient.getName());
      } else if (recipient.getName() == null && name == null) {
        return type.equals(recipient.getType());
      } else {
        return false;
      }
	  } else {
	    return false;
	  }
  }

  private String name;

	private Type type;

	// ~ Constructors
	// ====================================================================================================

	public RepositoryObjectRecipient(String name) {
		this(name, Type.USER);
	}

	public RepositoryObjectRecipient(String name, Type type) {
		super();
		this.name = name;
		this.type = type;
	}

	// ~ Methods
	// =========================================================================================================

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

}
