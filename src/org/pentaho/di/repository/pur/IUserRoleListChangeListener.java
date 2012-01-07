/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.di.repository.pur;

public interface IUserRoleListChangeListener {

  /**
   * Event listener interface for user role list change events.
   */

     /**
      * Fired when the user role list change
      * 
      */
      void onChange();
}
