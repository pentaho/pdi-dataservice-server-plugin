/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
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
