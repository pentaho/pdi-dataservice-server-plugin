/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

import java.util.ArrayList;


public class UserRoleListChangeListenerCollection extends ArrayList<IUserRoleListChangeListener> implements java.io.Serializable {

  private static final long serialVersionUID = -7723158765985622583L; /* EESOURCE: UPDATE SERIALVERUID */

    /**
    * Fires a user role list change event to all listeners.
    * 
    */
    public void fireOnChange() {
      for (IUserRoleListChangeListener listener : this) {
        listener.onChange();
      }
    }
}
