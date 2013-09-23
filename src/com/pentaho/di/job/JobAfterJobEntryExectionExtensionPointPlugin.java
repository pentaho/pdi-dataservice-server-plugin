/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.di.job;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionExtension;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;

@ExtensionPoint(
    id="JobAfterJobEntryExectionExtensionPointPlugin",
    description="Save checkpoint information if one was reached",
    extensionPointId="JobBeforeJobEntryExecution"
  )
public class JobAfterJobEntryExectionExtensionPointPlugin implements ExtensionPointInterface {

  // private static Class<?> PKG = JobAfterJobEntryExectionExtensionPointPlugin.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof JobExecutionExtension)) return;
    JobExecutionExtension extension = (JobExecutionExtension)object;
    
    // The executeEntry set to false means we jumped to the entry, not executed it.
    // That in turn means we don't need to save the checkpoint information.
    //
    if (!extension.executeEntry) return;
    
    Job job = extension.job;
    JobEntryCopy jobEntryCopy = extension.jobEntryCopy;
    JobMeta jobMeta = extension.job.getJobMeta();
    
    CheckpointLogTable checkpointLogTable = JobRestartConst.getCheckpointLogTable(jobMeta);
    
    if (checkpointLogTable==null) return;
    
    boolean checkpoint = JobRestartConst.isCheckpoint(jobEntryCopy);

    // If this is a checkpoint, write to the checkpoint log table
    //
    if (checkpoint && checkpointLogTable.isDefined()) {
      JobRestartConst.writeCheckpointInformation(checkpointLogTable, job, jobEntryCopy, extension.result);
    }
  }

}
