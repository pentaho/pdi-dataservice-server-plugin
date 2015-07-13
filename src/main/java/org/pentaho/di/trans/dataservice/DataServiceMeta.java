/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice;

import com.google.common.base.Objects;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.sql.ServiceCacheMethod;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.List;

/**
 * This describes a (transformation) data service to the outside world.
 * It defines the name, picks the step to read from (or to write to), the caching method etc.
 *
 * @author matt
 */
@MetaStoreElementType(
  name = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_NAME,
  description = PentahoDefaults.KETTLE_DATA_SERVICE_ELEMENT_TYPE_DESCRIPTION )
public class DataServiceMeta {

  public static final String DATA_SERVICE_TRANSFORMATION_STEP_NAME = "step_name";
  public static final String DATA_SERVICE_TRANSFORMATION_FILENAME = "transformation_filename";
  public static final String DATA_SERVICE_TRANSFORMATION_REP_PATH = "transformation_rep_path";
  public static final String DATA_SERVICE_TRANSFORMATION_REP_OBJECT_ID = "transformation_rep_object_id";
  public static final String DATA_SERVICE_CACHE_METHOD = "cache_method";
  public static final String DATA_SERVICE_CACHE_MAX_AGE_MINUTES = "cache_max_age_minutes";
  public static final String PUSH_DOWN_OPT_META = "push_down_opt_meta";

  protected String name;

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_STEP_NAME )
  protected String stepname;

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_REP_OBJECT_ID )
  protected String transObjectId; // rep: by reference (1st priority)

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_REP_PATH )
  protected String transRepositoryPath; // rep: by name (2nd priority)

  @MetaStoreAttribute( key = DATA_SERVICE_TRANSFORMATION_FILENAME )
  protected String transFilename; // file (3rd priority)

  protected String id;

  @MetaStoreAttribute( key = DATA_SERVICE_CACHE_METHOD )
  protected ServiceCacheMethod cacheMethod = ServiceCacheMethod.None;

  @MetaStoreAttribute( key = DATA_SERVICE_CACHE_MAX_AGE_MINUTES )
  protected int cacheMaxAgeMinutes;

  @MetaStoreAttribute( key = PUSH_DOWN_OPT_META  )
  protected List<PushDownOptimizationMeta> pushDownOptimizationMeta;

  public DataServiceMeta() {
    this( null, null );
  }

  /**
   * @param name
   * @param stepname
   */
  public DataServiceMeta( String name, String stepname ) {
    this.name = name;
    this.stepname = stepname;
    pushDownOptimizationMeta = new ArrayList<PushDownOptimizationMeta>();
  }

  public boolean isDefined() {
    return !Const.isEmpty( name ) && !Const.isEmpty( stepname );
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname the stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * @return the (metastore) id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * @return the cacheMethod
   */
  public ServiceCacheMethod getCacheMethod() {
    return cacheMethod;
  }

  /**
   * @param cacheMethod the cacheMethod to set
   */
  public void setCacheMethod( ServiceCacheMethod cacheMethod ) {
    this.cacheMethod = cacheMethod;
  }

  public String getTransObjectId() {
    return transObjectId;
  }

  public void setTransObjectId( String transObjectId ) {
    this.transObjectId = transObjectId;
  }

  public String getTransRepositoryPath() {
    return transRepositoryPath;
  }

  public void setTransRepositoryPath( String transRepositoryPath ) {
    this.transRepositoryPath = transRepositoryPath;
  }

  public String getTransFilename() {
    return transFilename;
  }

  public void setTransFilename( String transFilename ) {
    this.transFilename = transFilename;
  }

  public int getCacheMaxAgeMinutes() {
    return cacheMaxAgeMinutes;
  }

  public void setCacheMaxAgeMinutes( int cacheMaxAgeMinutes ) {
    this.cacheMaxAgeMinutes = cacheMaxAgeMinutes;
  }

  /**
   * Try to look up the transObjectId for transformation which are referenced by path
   * and set transRepositoryPath if found.
   * Caller is responsible for determining whether an oid was actually set.
   * @param repository The repository to use.
   * @throws KettleException
   */
  public ObjectId lookupTransObjectId( Repository repository ) throws KettleException {
    if ( transObjectId != null ) {
      return new StringObjectId( transObjectId );
    }
    if ( repository != null && StringUtils.isNotEmpty( transRepositoryPath ) ) {
      // see if there is a path specified to a repository name
      //
      String path = "/";
      String name = transRepositoryPath;
      int lastSlashIndex = name.lastIndexOf( '/' );
      if ( lastSlashIndex >= 0 ) {
        path = transRepositoryPath.substring( 0, lastSlashIndex + 1 );
        name = transRepositoryPath.substring( lastSlashIndex + 1 );
      }
      RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
      RepositoryDirectoryInterface rd = tree.findDirectory( path );
      if ( rd == null ) {
        rd = tree; // root
      }
      ObjectId oid = repository.getTransformationID( name, rd );
      if ( oid != null ) {
        transObjectId = oid.getId();
        return oid;
      }
    }
    return null;
  }

  public TransMeta lookupTransMeta( Repository repository ) throws KettleException {
    if ( StringUtils.isNotEmpty( transFilename )) {
      return new TransMeta( transFilename );
    } else if ( repository != null ) {
      ObjectId objectId = lookupTransObjectId( repository );
      return repository.loadTransformation( objectId, null );
    } else {
      throw new KettleException( "Could not load TransMeta for DataService " + getName() );
    }
  }

  public List<PushDownOptimizationMeta> getPushDownOptimizationMeta() {
    return pushDownOptimizationMeta;
  }

  public void setPushDownOptimizationMeta( List<PushDownOptimizationMeta> pushDownOptimizationMeta ) {
    this.pushDownOptimizationMeta = pushDownOptimizationMeta;
  }

  public static CacheKey createCacheKey( TransMeta transMeta, String stepName ) {
    String identifier = "";
    Repository repository = transMeta.getRepository();
    if ( repository != null ) {
      if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
        ObjectId objectId = transMeta.getObjectId();
        if ( objectId != null ) {
          identifier = objectId.getId();
        }
      }
      if ( Const.isEmpty( identifier ) ) {
        identifier = transMeta.getPathAndName();
      }
    } else {
      identifier = transMeta.getFilename();
    }

    return new CacheKey( identifier, stepName );
  }

  public static final class CacheKey {

    private final String identifier;
    private final String stepName;

    public CacheKey( String identifier, String stepName ) {
      this.identifier = identifier;
      this.stepName = stepName;
    }

    @Override
    public boolean equals( Object obj ) {
      if ( this == obj ) {
        return true;
      }
      if ( obj == null || getClass() != obj.getClass() ) {
        return false;
      }

      final CacheKey cacheKey = (CacheKey) obj;
      return Objects.equal( identifier, cacheKey.identifier ) && Objects.equal( stepName, cacheKey.stepName );
    }

    @Override
    public int hashCode() {
      return Objects.hashCode( identifier, stepName );
    }
  }
}
