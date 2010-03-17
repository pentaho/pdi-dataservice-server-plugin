package org.pentaho.di.ui.repository.pur;

import org.pentaho.di.repository.pur.PurRepositoryMeta;

public interface IRepositoryConfigDialogCallback {
  /**
   * On a successful configuration of a repostory, this method is invoked
   * @param repositoryMeta
   */
  void onSuccess(PurRepositoryMeta repositoryMeta);
  /**
   * On a user cancelation from the repository configuration dialog, this
   * method is invoked
   */
  void onCancel();
  /**
   * On any error caught during the repository configuration process, this method is
   * invoked 
   * @param t
   */
  void onError(Throwable t);
}


