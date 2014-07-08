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

package org.pentaho.di.repository.pur;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.RepositoryImporter;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.TransMeta;

public class PurRepositoryImporter extends RepositoryImporter implements java.io.Serializable {
  private static final long serialVersionUID = 2853810493291696227L; /* EESOURCE: UPDATE SERIALVERUID */

  private PurRepository rep;

  private Map<Class<?>, List<?>> sharedObjectsByType = null;

  public PurRepositoryImporter( PurRepository repository ) {
    super( repository, new LogChannel( "Repository import" ) );
    this.rep = repository;
  }

  @Override
  protected <T extends SharedObjectInterface> List<T> getSharedObjects( Class<T> clazz ) {
    List<T> result = new ArrayList<T>();
    List<?> typeList = sharedObjectsByType.get( clazz );
    if ( typeList != null ) {
      for ( Object obj : typeList ) {
        result.add( clazz.cast( obj ) );
      }
    }
    return result;
  }

  private void populateSharedObjectsMap() throws KettleException {
    sharedObjectsByType = new HashMap<Class<?>, List<?>>();
    for ( Entry<RepositoryObjectType, List<? extends SharedObjectInterface>> entry : rep.loadAndCacheSharedObjects()
        .entrySet() ) {
      Class<?> clazz = null;
      switch ( entry.getKey() ) {
        case DATABASE:
          clazz = DatabaseMeta.class;
          break;
        case SLAVE_SERVER:
          clazz = SlaveServer.class;
          break;
        case PARTITION_SCHEMA:
          clazz = PartitionSchema.class;
          break;
        case CLUSTER_SCHEMA:
          clazz = ClusterSchema.class;
          break;
        default:
          break;
      }
      if ( clazz != null ) {
        sharedObjectsByType.put( clazz, new ArrayList<Object>( entry.getValue() ) );
      }
    }
  }
  
  @Override
  protected void loadSharedObjects() throws KettleException {
    // Noop
  }

  @Override
  protected void replaceSharedObjects( TransMeta transMeta ) throws KettleException {
    populateSharedObjectsMap();
    super.replaceSharedObjects( transMeta );
  }

  @Override
  protected void replaceSharedObjects( JobMeta jobMeta ) throws KettleException {
    populateSharedObjectsMap();
    super.replaceSharedObjects( jobMeta );
  }

  @Override
  protected boolean equals( DatabaseMeta databaseMeta, DatabaseMeta databaseMeta2 ) {
    return rep.getDatabaseMetaTransformer().equals( databaseMeta, databaseMeta2 );
  }

  @Override
  protected boolean equals( SlaveServer slaveServer, SlaveServer slaveServer2 ) {
    return rep.getSlaveTransformer().equals( slaveServer, slaveServer2 );
  }

  @Override
  protected boolean equals( ClusterSchema clusterSchema, ClusterSchema clusterSchema2 ) {
    return rep.getClusterTransformer().equals( clusterSchema, clusterSchema2 );
  }

  @Override
  protected boolean equals( PartitionSchema partitionSchema, PartitionSchema partitionSchema2 ) {
    return rep.getPartitionSchemaTransformer().equals( partitionSchema, partitionSchema2 );
  }

  @Override
  protected void saveTransMeta( TransMeta transMeta ) throws KettleException {
    rep.saveTrans0( transMeta, getVersionComment(), null, true, false, false, false, false );
  }

  @Override
  protected void saveJobMeta( JobMeta jobMeta ) throws KettleException {
    rep.saveJob0( jobMeta, getVersionComment(), true, false, false, false, false );
  }
}
