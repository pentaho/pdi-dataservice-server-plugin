package org.pentaho.di.repository;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;


public interface ITrashService extends IRepositoryService {

  void delete(final List<ObjectId> ids) throws KettleException;

  void undelete(final List<ObjectId> ids) throws KettleException;

  List<RepositoryElement> getTrash() throws KettleException;
}
