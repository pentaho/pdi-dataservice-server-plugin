package com.pentaho.di.trans.dataservice;

import java.util.Arrays;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransDialogPlugin;
import org.pentaho.di.ui.trans.dialog.TransDialogPluginInterface;

@TransDialogPlugin(
    id="DataServiceTransDialogTab",
    name="Data service transformation dialog tab plugin",
    description="This plugin makes sure there's an extra 'Data Service' tab in the transformation settings dialog",
    i18nPackageName="com.pentaho.di.trans.dataservice"
    )
public class DataServiceTransDialogTab implements TransDialogPluginInterface {

  private static Class<?> PKG = DataServiceTransDialogTab.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  private CTabItem wDataServiceTab;
  private TextVar wServiceName;
  private CCombo wServiceStep;
  
  /*
  private Button wServiceOutput;
  private Button wServiceAllowOptimization;
  private CCombo wServiceCacheMethod;
  */
  
  @Override
  public void addTab(TransMeta transMeta, Shell shell, CTabFolder wTabFolder) {
    PropsUI props = PropsUI.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;
    
    wDataServiceTab = new CTabItem(wTabFolder, SWT.NONE);
    wDataServiceTab.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceTab.Label")); 

    Composite wDataServiceComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wDataServiceComp);

    FormLayout dataServiceLayout = new FormLayout();
    dataServiceLayout.marginWidth = Const.FORM_MARGIN;
    dataServiceLayout.marginHeight = Const.FORM_MARGIN;
    wDataServiceComp.setLayout(dataServiceLayout);

    // 
    // Service name
    //
    Label wlServiceName = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceName.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceName.Label")); 
    wlServiceName.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceName.Tooltip"));
    props.setLook(wlServiceName);
    FormData fdlServiceName = new FormData();
    fdlServiceName.left = new FormAttachment(0, 0);
    fdlServiceName.right = new FormAttachment(middle, -margin);
    fdlServiceName.top = new FormAttachment(0, 0);
    wlServiceName.setLayoutData(fdlServiceName);
    wServiceName = new TextVar(transMeta, wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE);
    wServiceName.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceName.Tooltip"));
    props.setLook(wServiceName);
    FormData fdServiceName = new FormData();
    fdServiceName.left = new FormAttachment(middle, 0);
    fdServiceName.right = new FormAttachment(100, 0);
    fdServiceName.top = new FormAttachment(0, 0);
    wServiceName.setLayoutData(fdServiceName);
    Control lastControl = wServiceName;

    // 
    // Service step
    //
    Label wlServiceStep = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceStep.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceStep.Label")); 
    wlServiceStep.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceStep.Tooltip"));
    props.setLook(wlServiceStep);
    FormData fdlServiceStep = new FormData();
    fdlServiceStep.left = new FormAttachment(0, 0);
    fdlServiceStep.right = new FormAttachment(middle, -margin);
    fdlServiceStep.top = new FormAttachment(lastControl, margin);
    wlServiceStep.setLayoutData(fdlServiceStep);
    wServiceStep = new CCombo(wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE);
    wServiceStep.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceStep.Tooltip"));
    props.setLook(wServiceStep);
    FormData fdServiceStep = new FormData();
    fdServiceStep.left = new FormAttachment(middle, 0);
    fdServiceStep.right = new FormAttachment(100, 0);
    fdServiceStep.top = new FormAttachment(lastControl, margin);
    wServiceStep.setLayoutData(fdServiceStep);
    String[] stepnames = transMeta.getStepNames();
    Arrays.sort(stepnames);
    wServiceStep.setItems(stepnames);
    lastControl = wServiceStep;

    /*
    // 
    // Cache method
    //
    Label wlServiceCacheMethod = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceCacheMethod.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Label")); 
    wlServiceCacheMethod.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Tooltip"));
    props.setLook(wlServiceCacheMethod);
    FormData fdlServiceCacheMethod = new FormData();
    fdlServiceCacheMethod.left = new FormAttachment(0, 0);
    fdlServiceCacheMethod.right = new FormAttachment(middle, -margin);
    fdlServiceCacheMethod.top = new FormAttachment(wServiceStep, margin);
    wlServiceCacheMethod.setLayoutData(fdlServiceCacheMethod);
    wServiceCacheMethod = new CCombo(wDataServiceComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE);
    wServiceCacheMethod.setToolTipText(BaseMessages.getString(PKG, "TransDialog.DataServiceCacheMethod.Tooltip"));
    props.setLook(wServiceCacheMethod);
    FormData fdServiceCacheMethod = new FormData();
    fdServiceCacheMethod.left = new FormAttachment(middle, 0);
    fdServiceCacheMethod.right = new FormAttachment(100, 0);
    fdServiceCacheMethod.top = new FormAttachment(wServiceStep, margin);
    wServiceCacheMethod.setLayoutData(fdServiceCacheMethod);
    String[] cacheMethodDescriptions = ServiceCacheMethod.getDescriptions();
    Arrays.sort(cacheMethodDescriptions);
    wServiceCacheMethod.setItems(cacheMethodDescriptions);
    
    // 
    // output service?
    //
    Label wlServiceOutput = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceOutput.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceOutput.Label")); 
    props.setLook(wlServiceOutput);
    FormData fdlServiceOutput = new FormData();
    fdlServiceOutput.left = new FormAttachment(0, 0);
    fdlServiceOutput.right = new FormAttachment(middle, -margin);
    fdlServiceOutput.top = new FormAttachment(wServiceCacheMethod, margin);
    wlServiceOutput.setLayoutData(fdlServiceOutput);
    wlServiceOutput.setEnabled(false);
    wServiceOutput = new Button(wDataServiceComp, SWT.CHECK);
    props.setLook(wServiceOutput);
    FormData fdServiceOutput = new FormData();
    fdServiceOutput.left = new FormAttachment(middle, 0);
    fdServiceOutput.right = new FormAttachment(100, 0);
    fdServiceOutput.top = new FormAttachment(wServiceCacheMethod, margin);
    wServiceOutput.setLayoutData(fdServiceOutput);
    wServiceOutput.setEnabled(false);

    // 
    // Allow optimisation?
    //
    Label wlServiceAllowOptimization = new Label(wDataServiceComp, SWT.LEFT);
    wlServiceAllowOptimization.setText(BaseMessages.getString(PKG, "TransDialog.DataServiceAllowOptimization.Label")); 
    props.setLook(wlServiceAllowOptimization);
    FormData fdlServiceAllowOptimization = new FormData();
    fdlServiceAllowOptimization.left = new FormAttachment(0, 0);
    fdlServiceAllowOptimization.right = new FormAttachment(middle, -margin);
    fdlServiceAllowOptimization.top = new FormAttachment(wServiceOutput, margin);
    wlServiceAllowOptimization.setLayoutData(fdlServiceAllowOptimization);
    wlServiceAllowOptimization.setEnabled(false);
    wServiceAllowOptimization = new Button(wDataServiceComp, SWT.CHECK);
    props.setLook(wServiceAllowOptimization);
    FormData fdServiceAllowOptimization = new FormData();
    fdServiceAllowOptimization.left = new FormAttachment(middle, 0);
    fdServiceAllowOptimization.right = new FormAttachment(100, 0);
    fdServiceAllowOptimization.top = new FormAttachment(wServiceOutput, margin);
    wServiceAllowOptimization.setLayoutData(fdServiceAllowOptimization);
    wServiceAllowOptimization.setEnabled(false);
    */

    FormData fdDataServiceComp = new FormData();
    fdDataServiceComp.left = new FormAttachment(0, 0);
    fdDataServiceComp.top = new FormAttachment(0, 0);
    fdDataServiceComp.right = new FormAttachment(100, 0);
    fdDataServiceComp.bottom = new FormAttachment(100, 0);
    wDataServiceComp.setLayoutData(fdDataServiceComp);

    wDataServiceComp.layout();
    wDataServiceTab.setControl(wDataServiceComp);
  }

  @Override
  public void getData(TransMeta transMeta) throws KettleException {
    try {
      // Data service metadata
      //
      DataServiceMeta dataService = DataServiceMetaStoreUtil.fromTransMeta(transMeta, transMeta.getMetaStore());
      if (dataService != null) {
        wServiceName.setText(Const.NVL(dataService.getName(), ""));
        wServiceStep.setText(Const.NVL(dataService.getStepname(), ""));
        // wServiceOutput.setSelection(dataService.isOutput());
        // wServiceAllowOptimization.setSelection(dataService.isOptimizationAllowed());
        // wServiceCacheMethod.setText(dataService.getCacheMethod()==null ? "" : dataService.getCacheMethod().getDescription());
      }
    } catch(Exception e) {
      throw new KettleException("Unable to load data service", e);
    }
  }

  @Override
  public void ok(TransMeta transMeta) throws KettleException {
    
    try {
      // Get data service details...
      //
      DataServiceMeta dataService = new DataServiceMeta();
      dataService.setName(wServiceName.getText());
      dataService.setStepname(wServiceStep.getText());
      // dataService.setOutput(wServiceOutput.getSelection());
      // dataService.setOptimizationAllowed(wServiceAllowOptimization.getSelection());
      // dataService.setCacheMethod(ServiceCacheMethod.getMethodByDescription(wServiceCacheMethod.getText()));
      
      if (!Const.isEmpty(dataService.getName()) && !Const.isEmpty(dataService.getStepname())) {
        LogChannel.GENERAL.logBasic("Saving data service in meta store '"+transMeta.getMetaStore()+"'");
        DataServiceMetaStoreUtil.toTransMeta(transMeta, transMeta.getMetaStore(), dataService, true);
        transMeta.setChanged();
      }
      
    } catch(Exception e) {
      throw new KettleException("Error reading data service metadata", e);
    }

  }

  @Override
  public CTabItem getTab() {
    return wDataServiceTab;
  }
}
