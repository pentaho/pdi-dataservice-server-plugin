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
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

public class PurRepositoryDialog implements RepositoryDialogInterface {
  private RepositoryMeta repositoryMeta;

  private RepositoriesMeta repositoriesMeta;

  private Shell parent;

  private int style;

  private static final Class<?> CLZ = PurRepositoryDialog.class;

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
      return BaseMessages.getString(CLZ, key);
    }

  };

  public PurRepositoryDialog(Shell parent, int style, RepositoryMeta repositoryMeta,
      RepositoriesMeta repositoriesMeta) {
    this.parent = parent;
    this.repositoriesMeta = repositoriesMeta;
    this.repositoryMeta = repositoryMeta;
    this.style = style;
  }

  public RepositoryMeta open(MODE mode) {
    try {
      SwtXulLoader swtLoader = new SwtXulLoader();
      swtLoader.setOuterContext(parent);
      swtLoader.registerClassLoader(getClass().getClassLoader());
      container = swtLoader.loadXul(
          "org/pentaho/di/ui/repository/pur/xul/pur-repository-config-dialog.xul", resourceBundle); //$NON-NLS-1$
      final XulRunner runner = new SwtXulRunner();
      runner.addContainer(container);
      parent.addDisposeListener(new DisposeListener(){

        public void widgetDisposed(DisposeEvent arg0) {
          hide();
        }
        
      });
      repositoryConfigController.setMode(mode);
      repositoryConfigController.setMessages(resourceBundle);
      repositoryConfigController.setRepositoryMeta(repositoryMeta);
      repositoryConfigController.setCallback(new IRepositoryConfigDialogCallback() {

        public void onSuccess(PurRepositoryMeta meta) {
          repositoryMeta = meta;
          hide();
        }

        public void onError(Throwable t) {
          SpoonFactory.getInstance().messageBox(t.getLocalizedMessage(), resourceBundle.getString("RepositoryConfigDialog.InitializationFailed"), false, Const.ERROR); //$NON-NLS-1$
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
        SpoonFactory.getInstance().messageBox(e.getLocalizedMessage(), resourceBundle.getString("RepositoryConfigDialog.InitializationFailed"), false, Const.ERROR); //$NON-NLS-1$
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

}
