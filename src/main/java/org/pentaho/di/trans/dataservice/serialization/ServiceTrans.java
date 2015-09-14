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

package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.io.File;
import java.util.List;
import java.util.Set;

@MetaStoreElementType(
  name = "Data Service Transformation",
  description = "Pointer to a saved transformation that supplies a data service" )
public class ServiceTrans implements MetaStoreElement {
  private String name;

  @MetaStoreAttribute( key = "trans_references" )
  private List<Reference> references = Lists.newArrayList();

  public static ServiceTrans create( DataServiceMeta dataServiceMeta ) {
    return create( dataServiceMeta.getName(), dataServiceMeta.getServiceTrans() );
  }

  public static ServiceTrans create( String name, TransMeta transMeta ) {
    ServiceTrans serviceTrans = new ServiceTrans();
    serviceTrans.setName( name );
    serviceTrans.getReferences().addAll( references( transMeta ) );
    return serviceTrans;
  }

  public static List<Reference> references( TransMeta transMeta ) {
    List<Reference> references = Lists.newArrayList();
    Repository repository = transMeta.getRepository();
    if ( transMeta.getRepository() != null ) {
      if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
        ObjectId oid = transMeta.getObjectId();
        if ( oid != null ) {
          references.add( new Reference( StorageMethod.REPO_ID, oid.getId() ) );
        }
      }
      references.add( new Reference( StorageMethod.REPO_PATH, transMeta.getPathAndName() ) );
    } else {
      references.add( new Reference( StorageMethod.FILE, transMeta.getFilename() ) );
    }
    return references;
  }

  public static Predicate<ServiceTrans> isReferenceTo( final TransMeta transMeta ) {
    return new Predicate<ServiceTrans>() {
      final Set<Reference> references = ImmutableSet.copyOf( references( transMeta ) );

      @Override public boolean apply( ServiceTrans serviceTrans ) {
        return Iterables.any( serviceTrans.getReferences(), Predicates.in( references ) );
      }
    };
  }

  public static Predicate<ServiceTrans> isValid( final Repository repository ) {
    return new Predicate<ServiceTrans>(){
      @Override public boolean apply( ServiceTrans input ) {
        return Iterables.any( input.getReferences(), Reference.isValid( repository ) );
      }
    };
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName( String name ) {
    this.name = name;
  }

  public List<Reference> getReferences() {
    return references;
  }

  @Override public String toString() {
    return Objects.toStringHelper( this )
      .add( "name", name )
      .add( "references", references )
      .toString();
  }

  public static class Reference {
    @MetaStoreAttribute( key = "transformation_location" )
    private String location;

    @MetaStoreAttribute( key = "transformation_storage_method" )
    private StorageMethod method;

    /**
     * Zero-arg constructor for MetaStoreFactory
     */
    @Deprecated
    public Reference() {
    }

    public Reference( StorageMethod method, String location ) {
      this.location = location;
      this.method = method;
    }

    public static Predicate<Reference> isValid( final Repository repository ) {
      return new Predicate<Reference>() {
        @Override public boolean apply( Reference reference ) {
          return reference.exists( repository );
        }
      };
    }

    public String getLocation() {
      return location;
    }

    public void setLocation( String location ) {
      this.location = location;
    }

    public StorageMethod getMethod() {
      return method;
    }

    public void setMethod( StorageMethod method ) {
      this.method = method;
    }

    @Override public String toString() {
      return Objects.toStringHelper( this )
        .add( "method", method )
        .add( "location", location )
        .toString();
    }

    @Override public int hashCode() {
      return Objects.hashCode( location, method );
    }

    @Override public boolean equals( Object o ) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }
      Reference reference = (Reference) o;
      return Objects.equal( location, reference.location ) && Objects.equal( method, reference.method );
    }

    public boolean exists( Repository repository ) {
      return method.exists( repository, location );
    }

    public TransMeta load( Repository repository ) throws KettleException {
      return method.load( repository, location );
    }
  }
  public enum StorageMethod {
    FILE {
      @Override public boolean exists( Repository repository, String location ) {
        return new File( location ).exists();
      }

      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        return new TransMeta( location );
      }
    },
    REPO_PATH {
      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        String path;
        String name;

        int lastSlashIndex = location.lastIndexOf( '/' );
        if ( lastSlashIndex >= 0 ) {
          path = location.substring( 0, lastSlashIndex + 1 );
          name = location.substring( lastSlashIndex + 1 );
        } else {
          path = "/";
          name = location;
        }

        RepositoryDirectoryInterface root = repository.loadRepositoryDirectoryTree();
        RepositoryDirectoryInterface rd = root.findDirectory( path );
        if ( rd == null ) {
          rd = root; // root
        }
        return repository.loadTransformation( repository.getTransformationID( name, rd ), null );
      }
    },
    REPO_ID {
      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        return repository.loadTransformation( new StringObjectId( location ), null );
      }
    };

    public abstract TransMeta load( Repository repository, String location ) throws KettleException;

    public boolean exists( Repository repository, String location ) {
      // Assume true, unless we know otherwise.
      return true;
    }
  }
}
