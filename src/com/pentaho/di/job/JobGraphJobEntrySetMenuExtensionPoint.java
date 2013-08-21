package com.pentaho.di.job;

import org.eclipse.jface.action.Action;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.job.JobGraphJobEntryMenuExtension;
import org.pentaho.ui.xul.components.XulMenuitem;
import org.pentaho.ui.xul.containers.XulMenupopup;
import org.pentaho.ui.xul.jface.tags.JfaceMenuitem;

@ExtensionPoint(
    id = "JobGraphJobEntrySetMenuExtensionPoint",
    extensionPointId = "JobGraphJobEntrySetMenu",
    description = "This plugin inserts the checkpoint menu item in the job entry right click popup menu"
    )
public class JobGraphJobEntrySetMenuExtensionPoint implements ExtensionPointInterface {
  private static Class<?> PKG = JobGraphJobEntrySetMenuExtensionPoint.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public static String ITEM_ID = "mark-as-checkpoint";
  
  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    
    if (!(object instanceof JobGraphJobEntryMenuExtension)) return;
    final JobGraphJobEntryMenuExtension extension = (JobGraphJobEntryMenuExtension) object;

    CheckpointLogTable checkpointLogTable = JobRestartConst.getCheckpointLogTable(extension.jobMeta);
    boolean enabled = checkpointLogTable!=null && checkpointLogTable.isDefined();
    
    XulMenuitem checkpointItem = (XulMenuitem) extension.doc.getElementById(ITEM_ID);
    if (checkpointItem!=null) {
      checkpointItem.setDisabled(!enabled);
      
      boolean isCheckpoint =JobRestartConst.isCheckpoint(extension.jobEntry); 
      setLabel((JfaceMenuitem) checkpointItem, !isCheckpoint);
      
      return; // already there...
    }
    
    XulMenupopup popupMenu = (XulMenupopup) extension.doc.getElementById("job-graph-entry");
    
    Action action = new Action("mark-as-checkpoint", Action.AS_DROP_DOWN_MENU) {
      public void run() {
        boolean isCheckpoint =JobRestartConst.isCheckpoint(extension.jobEntry); 
        JobRestartConst.setCheckpoint(extension.jobEntry, !isCheckpoint);
        extension.jobEntry.setChanged();
        extension.jobGraph.redraw();
        Spoon.getInstance().setShellText();
      }
    }; 
    
    JfaceMenuitem child = new JfaceMenuitem(null, popupMenu, extension.xulDomContainer, 
        ITEM_ID, 5, action);
    boolean isCheckpoint =JobRestartConst.isCheckpoint(extension.jobEntry); 
    setLabel(child, !isCheckpoint);
    child.setInsertafter("job-graph-entry-parallel");
    child.setId(ITEM_ID);
    child.setDisabled(!enabled);
  }

  private void setLabel(JfaceMenuitem child, boolean enabled) {
    if (enabled) {
      child.setLabel(BaseMessages.getString(PKG, "JobGraphJobEntrySetMenuExtensionPoint.MarkCheckpoint.Label"));
    } else {
      child.setLabel(BaseMessages.getString(PKG, "JobGraphJobEntrySetMenuExtensionPoint.UnMarkCheckpoint.Label"));
    }
  }
}
