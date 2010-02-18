package org.pentaho.di.repository.pur;

import java.util.ArrayList;


public class UserRoleListChangeListenerCollection extends ArrayList<IUserRoleListChangeListener> {
    private static final long serialVersionUID = 1L;

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
