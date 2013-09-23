/*!
* The Pentaho proprietary code is licensed under the terms and conditions
* of the software license agreement entered into between the entity licensing
* such code and Pentaho Corporation.
*
* This software costs money - it is not free
*
* Copyright 2002 - 2013 Pentaho Corporation.  All rights reserved.
*/

package org.pentaho.di.ui.repository.pur.services;

import java.util.Date;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.IRepositoryService;
import org.pentaho.di.repository.ObjectId;
/**
 * Repository Service used to add a trash bin feature to the repository
 * @author mlowery
 *
 */

public interface ITrashService extends IRepositoryService {

  /**
   * Delete the list of files matching ids 
   * @param ids
   * @throws KettleException if something bad happens
   */
  void delete(final List<ObjectId> ids) throws KettleException;

  /**
   * Un deletes the list of files matching the ids
   * @param ids
   * @throws KettleException
   */
  void undelete(final List<ObjectId> ids) throws KettleException;

  /**
   * Retrieves the current trash items for the user
   * @return
   * @throws KettleException
   */
  List<IDeletedObject> getTrash() throws KettleException;
  
  interface IDeletedObject {
    
    String getOriginalParentPath();
    
    Date getDeletedDate();
    
    /**
     * Directory ({@code null}), Transformation, or Job.
     */
    String getType();
    
    ObjectId getId();
    
    String getName();
    
    
  }
}
