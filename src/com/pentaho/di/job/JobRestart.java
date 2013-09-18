null
null
package com.pentaho.di.job;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogTableInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.job.dialog.JobDialogPlugin;
import org.pentaho.di.ui.job.dialog.JobDialogPluginInterface;

@JobDialogPlugin(
    id="JobRestart",
    name="Job restart",
    description="Handles the UI part of the job restart and transaction support settings"
    )
public class JobRestart implements JobDialogPluginInterface {

  private static Class<?> PKG = JobRestart.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  private CTabItem wRestartTab;

  private Button wUniqueConnections;

  @Override
  public void addTab(JobMeta jobMeta, Shell shell, CTabFolder wTabFolder) {
    PropsUI props = PropsUI.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;
    
    wRestartTab=new CTabItem(wTabFolder, SWT.NONE);
    wRestartTab.setText(BaseMessages.getString(PKG, "JobRestart.RestartTab.Label")); 

    FormLayout LogLayout = new FormLayout ();
    LogLayout.marginWidth  = Const.MARGIN;
    LogLayout.marginHeight = Const.MARGIN;
        
    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSettingsComp);
    wSettingsComp.setLayout(LogLayout);

    Label wlUniqueConnections = new Label(wSettingsComp, SWT.RIGHT);
    wlUniqueConnections.setText(BaseMessages.getString(PKG, "JobRestart.UniqueConnections.Label"));
    props.setLook(wlUniqueConnections);
    FormData fdlUniqueConnections = new FormData();
    fdlUniqueConnections.left = new FormAttachment(0, 0);
    fdlUniqueConnections.top  = new FormAttachment(0, 0);
    fdlUniqueConnections.right= new FormAttachment(middle, -margin);
    wlUniqueConnections.setLayoutData(fdlUniqueConnections);
    wUniqueConnections=new Button(wSettingsComp, SWT.CHECK);
    props.setLook(wUniqueConnections);
    wUniqueConnections.setToolTipText(BaseMessages.getString(PKG, "JobRestart.UniqueConnections.Tooltip"));
    FormData fdUniqueConnections = new FormData();
    fdUniqueConnections.left = new FormAttachment(middle, 0);
    fdUniqueConnections.top  = new FormAttachment(0, 0);
    fdUniqueConnections.right= new FormAttachment(100, 0);
    wUniqueConnections.setLayoutData(fdUniqueConnections);

    FormData fdLogComp = new FormData();
    fdLogComp.left  = new FormAttachment(0, 0);
    fdLogComp.top   = new FormAttachment(0, 0);
    fdLogComp.right = new FormAttachment(100, 0);
    fdLogComp.bottom= new FormAttachment(100, 0);
    wSettingsComp.setLayoutData(fdLogComp);
      
    wSettingsComp.layout();
    wRestartTab.setControl(wSettingsComp);
          
    /////////////////////////////////////////////////////////////
    /// END OF LOG TAB
    /////////////////////////////////////////////////////////////
  }

  @Override
  public void getData(JobMeta jobMeta) {
    String uniqueString = jobMeta.getAttribute(JobRestartConst.JOB_RESTART_GROUP, 
        JobRestartConst.JOB_DIALOG_ATTRIBUTE_UNIQUE_CONNECTIONS);
    wUniqueConnections.setSelection("Y".equalsIgnoreCase(uniqueString));
  }

  @Override
  public void ok(JobMeta jobMeta) {
    jobMeta.setAttribute(JobRestartConst.JOB_RESTART_GROUP, 
        JobRestartConst.JOB_DIALOG_ATTRIBUTE_UNIQUE_CONNECTIONS, wUniqueConnections.getSelection()?"Y":"N");
    jobMeta.setChanged();
  }

  @Override
  public void showLogTableOptions(JobMeta jobMeta, LogTableInterface logTable, Composite wLogOptionsComposite) {
    // Show the check-point log table if this is the one selected...
    //
    
  }
}
