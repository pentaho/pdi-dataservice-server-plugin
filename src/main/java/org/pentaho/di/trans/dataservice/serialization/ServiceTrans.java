package org.pentaho.di.trans.dataservice.serialization;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "Data Service Transformation",
  description = "Pointer to a saved transformation that supplies a data service" )
public class ServiceTrans {
  private String name;

  @MetaStoreAttribute( key = "trans_reference" )
  private Reference reference;

  public static ServiceTrans create( String name, TransMeta transMeta ) {
    ServiceTrans serviceTrans = new ServiceTrans();
    serviceTrans.setName( name );
    serviceTrans.setReference( create( transMeta ) );
    return serviceTrans;
  }

  public static Reference create( TransMeta transMeta ) {
    Repository repository = transMeta.getRepository();
    if ( transMeta.getRepository() == null ) {
      return new Reference( StorageMethod.FILE, transMeta.getFilename() );
    } else if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
      ObjectId oid = transMeta.getObjectId();
      if ( oid != null ) {
        return new Reference( StorageMethod.REPO_ID, oid.getId() );
      }
    }
    return new Reference( StorageMethod.REPO_PATH, transMeta.getPathAndName() );
  }

  public static ObjectId getObjectId( Repository repository, String transRepositoryPath ) throws KettleException {
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
    return repository.getTransformationID( name, rd );
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public void setReference( Reference reference ) {
    this.reference = reference;
  }

  public Reference getReference() {
    return reference;
  }

  public static class Reference {
    @MetaStoreAttribute( key = "transformation_location" )
    private String location;

    @MetaStoreAttribute( key = "transformation_storage_method" )
    private StorageMethod method;

    @Deprecated
    public Reference() {
    }

    public Reference( StorageMethod method, String location ) {
      this.location = location;
      this.method = method;
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

    public TransMeta load( Repository repository ) throws KettleException {
      return method.load( repository, location );
    }
  }
  public enum StorageMethod {
    FILE {
      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        return new TransMeta( location );
      }
    }, REPO_PATH {
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
    }, REPO_ID {
      @Override public TransMeta load( Repository repository, String location ) throws KettleException {
        return repository.loadTransformation( new StringObjectId( location ), null );
      }
    };

    public abstract TransMeta load( Repository repository, String location ) throws KettleException;
  }
}
