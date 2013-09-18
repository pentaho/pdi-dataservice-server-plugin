package com.pentaho.di.job;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.AreaOwner.AreaType;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EImage;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobPainterExtension;
import org.pentaho.di.job.entry.JobEntryCopy;

@ExtensionPoint(
    id="JobPainterArrowExtensionPointPlugin",
    extensionPointId="JobPainterArrow",
    description="Paint a checkered flag over a job hop in case a checkpoint is in effect"
  )
public class JobPainterArrowExtensionPointPlugin implements ExtensionPointInterface {
  private static Class<?> PKG = JobPainterArrowExtensionPointPlugin.class; // for i18n purposes, needed by Translator2!!

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof JobPainterExtension)) return;
    JobPainterExtension extension = (JobPainterExtension) object;
    
    CheckpointLogTable checkpointLogTable = JobRestartConst.getCheckpointLogTable(extension.jobMeta);
    if (checkpointLogTable!=null && !checkpointLogTable.isDefined()) return;
    boolean isCheckpoint = JobRestartConst.isCheckpoint(extension.jobHop.getFromEntry());
    if (!isCheckpoint) return;

    JobEntryCopy fromEntry = extension.jobHop.getFromEntry();

    double factor = 0.5;

    // in between 2 points
    int mx = (int) (extension.x1 + factor * (extension.x2 - extension.x1) / 2) - 8;
    int my = (int) (extension.y1 + factor * (extension.y2 - extension.y1) / 2) - 8;

    EImage hopsIcon = EImage.CHECKPOINT;
    extension.gc.drawImage(hopsIcon, mx, my);
    Point bounds = extension.gc.getImageBounds(hopsIcon);
    if (!extension.shadow) {
      extension.areaOwners.add(
          new AreaOwner(
            AreaType.CUSTOM, 
            mx, 
            my, 
            bounds.x, 
            bounds.y, 
            extension.offset,
            fromEntry,
            BaseMessages.getString(PKG, "JobRestartConst.JobEntry.Tooltip.Checkpoint", fromEntry, Const.CR)             
          )
        );
    }
  
  }

}
