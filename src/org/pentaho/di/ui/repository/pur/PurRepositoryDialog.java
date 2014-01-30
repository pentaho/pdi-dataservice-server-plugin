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

package org.pentaho.di.ui.repository.pur;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.pur.PurRepositoryMeta;
import org.pentaho.di.ui.repository.dialog.RepositoryDialogInterface;
import org.pentaho.di.ui.repository.pur.controller.RepositoryConfigController;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.components.XulMessageBox;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

public class PurRepositoryDialog implements RepositoryDialogInterface, java.io.Serializable {

  private static final long serialVersionUID = -4642068735574655692L; /* EESOURCE: UPDATE SERIALVERUID */
  protected RepositoryMeta repositoryMeta;
  protected RepositoriesMeta repositoriesMeta;
  protected RepositoriesMeta masterRepositoriesMeta;
  protected String masterRepositoryName;
  protected Shell parent;

  protected int style;

  private static final Class<?> PKG = PurRepositoryDialog.class;

  private static Log log = LogFactory.getLog(PurRepositoryDialog.class);

  private RepositoryConfigController repositoryConfigController = new RepositoryConfigController();

  private XulDomContainer container;
   
  private ResourceBundle resourceBundle = new ResourceBundle() {

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject(String key) {
      return BaseMessages.getString(PKG, key);
    }

  };

  public PurRepositoryDialog(Shell parent, int style, RepositoryMeta repositoryMeta, RepositoriesMeta repositoriesMeta) {
    this.parent = parent;
    this.repositoriesMeta = repositoriesMeta;
    this.repositoryMeta = repositoryMeta;
    this.style = style;
    this.masterRepositoriesMeta = repositoriesMeta.clone();
    this.masterRepositoryName = repositoryMeta.getName();
  }

  public RepositoryMeta open(final MODE mode) {
    try {
      SwtXulLoader swtLoader = new SwtXulLoader();
      swtLoader.setOuterContext(parent);
      swtLoader.registerClassLoader(getClass().getClassLoader());
      container = swtLoader.loadXul(
          "org/pentaho/di/ui/repository/pur/xul/pur-repository-config-dialog.xul", resourceBundle); //$NON-NLS-1$
      final XulRunner runner = new SwtXulRunner();
      runner.addContainer(container);
      parent.addDisposeListener(new DisposeListener() {

        public void widgetDisposed(DisposeEvent arg0) {
          hide();
        }

      });
      repositoryConfigController.setMessages(resourceBundle);
      repositoryConfigController.setRepositoryMeta(repositoryMeta);
      repositoryConfigController.setCallback(new IRepositoryConfigDialogCallback() {

        public void onSuccess(PurRepositoryMeta meta) {
          // If repository does not have a name then send back a null repository meta
          if (meta.getName() != null) {
            // If MODE is ADD then check if the repository name does not exist in the repository list then close this dialog
            // If MODE is EDIT then check if the repository name is the same as before if not check if the new name does
            // not exist in the repository. Otherwise return true to this method, which will mean that repository already exist
            if (mode == MODE.ADD) {
              if (masterRepositoriesMeta.searchRepository(meta.getName()) == null) {
                repositoryMeta = meta;
                hide();
              } else {
                displayRepositoryAlreadyExistMessage(meta.getName());
              }
            } else {
              if (masterRepositoryName.equals(meta.getName())) {
                repositoryMeta = meta;
                hide();
              } else if (masterRepositoriesMeta.searchRepository(meta.getName()) == null) {
                repositoryMeta = meta;
                hide();
              } else {
                displayRepositoryAlreadyExistMessage(meta.getName());
              }
            }
          }
        }

        public void onError(Throwable t) {
          SpoonFactory.getInstance().messageBox(t.getLocalizedMessage(),
              resourceBundle.getString("RepositoryConfigDialog.InitializationFailed"), false, Const.ERROR); //$NON-NLS-1$
          log.error(resourceBundle.getString("RepositoryConfigDialog.ErrorStartingXulApplication"), t);//$NON-NLS-1$
        }

        public void onCancel() {
          repositoryMeta = null;
          hide();
        }
      });
      container.addEventHandler(repositoryConfigController);

      try {
        runner.initialize();
        show();
      } catch (XulException e) {
        SpoonFactory.getInstance().messageBox(e.getLocalizedMessage(),
            resourceBundle.getString("RepositoryConfigDialog.InitializationFailed"), false, Const.ERROR); //$NON-NLS-1$
        log.error(resourceBundle.getString("RepositoryConfigDialog.ErrorStartingXulApplication"), e);//$NON-NLS-1$
      }
    } catch (XulException e) {
      log.error(resourceBundle.getString("RepositoryConfigDialog.ErrorStartingXulApplication"), e);//$NON-NLS-1$
    }
    return repositoryMeta;
  }

  public Composite getDialogArea() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById("repository-config-dialog"); //$NON-NLS-1$
    return (Composite) dialog.getManagedObject();
  }

  public void show() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById("repository-config-dialog"); //$NON-NLS-1$
    dialog.show();
  }

  public void hide() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById("repository-config-dialog"); //$NON-NLS-1$
    dialog.hide();
  }

  public Shell getShell() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById("repository-config-dialog"); //$NON-NLS-1$
    return (Shell) dialog.getRootObject();
  }

  private void  displayRepositoryAlreadyExistMessage(String name) {
    try {
      XulMessageBox messageBox = (XulMessageBox) container.getDocumentRoot().createElement("messagebox");
      messageBox.setTitle(resourceBundle.getString("Dialog.Error"));//$NON-NLS-1$
      messageBox.setAcceptLabel(resourceBundle.getString("Dialog.Ok"));//$NON-NLS-1$
      messageBox.setMessage(BaseMessages.getString(PKG, "PurRepositoryDialog.Dialog.ErrorIdExist.Message", name));//$NON-NLS-1$
      messageBox.open();

    } catch (XulException e) {
      throw new RuntimeException(e);
    }
  }
}
