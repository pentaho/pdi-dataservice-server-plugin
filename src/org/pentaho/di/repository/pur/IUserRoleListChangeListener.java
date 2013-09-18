/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
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
