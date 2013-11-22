package org.pentaho.di.repository.pur;

import java.lang.ref.SoftReference;

import org.pentaho.di.repository.RepositoryDirectoryInterface;

public class RootRef {
  private SoftReference<RepositoryDirectoryInterface> rootRef = null;

  public synchronized void setRef( RepositoryDirectoryInterface ref ) {
    rootRef = new SoftReference<RepositoryDirectoryInterface>( ref );
  }

  public synchronized RepositoryDirectoryInterface getRef() {
    return rootRef == null ? null : rootRef.get();
  }

  public synchronized void clearRef() {
    rootRef = null;
  }
}
