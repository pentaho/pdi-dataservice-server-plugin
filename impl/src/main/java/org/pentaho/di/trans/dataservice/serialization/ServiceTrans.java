/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.serialization;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryElementMetaInterface;
import org.pentaho.di.repository.RepositoryObject;
import org.pentaho.di.repository.RepositoryObjectType;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

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
    return new Predicate<ServiceTrans>() {
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
    return MoreObjects.toStringHelper( this )
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
      return MoreObjects.toStringHelper( this )
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
        return !Strings.isNullOrEmpty( location ) && new File( location ).exists();
      }

      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        if ( new File( location ).exists() ) {
          return new TransMeta( location );
        }
        return null;
      }
    },
    REPO_PATH {
      @Override public boolean exists( Repository repository, String location ) {
        try {
          return getTransId( repository, location ) != null;
        } catch ( KettleException ke ) {
          return false;
        }
      }

      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        TransMeta trans = null;
        ObjectId transId = getTransId( repository, location );
        if ( transId != null ) {
          RepositoryObject transInfo = repository.getObjectInformation( transId, RepositoryObjectType.TRANSFORMATION );
          if ( transInfo != null && !transInfo.isDeleted() ) {
            trans = repository.loadTransformation( transId, null );
          }
        }
        return trans;
      }

      private ObjectId getTransId( Repository repository, String location ) throws KettleException {
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

        ObjectId transId = repository.getTransformationID( name, rd );

        if ( transId == null ) {
          Optional<RepositoryElementMetaInterface> transInfo = repository
              .getTransformationObjects( rd.getObjectId(), true ).stream()
              .filter( m -> name.equals( m.getName() ) ).findFirst();
          if ( transInfo.isPresent() ) {
            transId = transInfo.get().getObjectId();
          }
        }

        return transId;
      }
    },
    REPO_ID {
      @Override public boolean exists( Repository repository, String location ) {
        try {
          return repository.getObjectInformation( new StringObjectId( location ), RepositoryObjectType.TRANSFORMATION ) != null;
        } catch ( KettleException ke ) {
          return false;
        }
      }

      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        TransMeta trans = null;
        ObjectId transId = new StringObjectId( location );
        RepositoryObject transInfo = repository.getObjectInformation( transId, RepositoryObjectType.TRANSFORMATION );
        if ( transInfo != null && !transInfo.isDeleted() ) {
          trans = repository.loadTransformation( transId, null );
        }
        return trans;
      }
    };

    public abstract TransMeta load( Repository repository, String location ) throws KettleException;

    public boolean exists( Repository repository, String location ) {
      // Assume true, unless we know otherwise.
      return true;
    }
  }
}
