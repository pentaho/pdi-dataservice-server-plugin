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

import java.util.Map;

import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.JobTracker;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobEntryResult;
import org.pentaho.di.job.JobExecutionExtension;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;

@ExtensionPoint(
    id="JobBeforeJobEntryExectionExtensionPointPlugin",
    description="Verify the execution of a job entry before execution, see if it's a checkpoint",
    extensionPointId="JobBeforeJobEntryExecution"
  )
public class JobBeforeJobEntryExectionExtensionPointPlugin implements ExtensionPointInterface {

  private static Class<?> PKG = JobBeforeJobEntryExectionExtensionPointPlugin.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof JobExecutionExtension)) return;
    
    JobExecutionExtension extension = (JobExecutionExtension)object;
    
    Job job = extension.job;
    JobEntryCopy jobEntryCopy = extension.jobEntryCopy;
    JobMeta jobMeta = extension.job.getJobMeta();
    
    CheckpointLogTable checkpointLogTable = JobRestartConst.getCheckpointLogTable(jobMeta);
    
    if (checkpointLogTable==null) return;
    
    JobEntryCopy checkpointJobEntry = JobRestartConst.getCheckpointJobEntry(job);
    Result checkpointResult = JobRestartConst.getCheckpointResult(job);
    Map<String, String> checkpointParameters = JobRestartConst.getCheckpointParameters(job);
    
    // See if we found a checkpoint from a previous execution...
    //
    if (checkpointJobEntry != null && checkpointJobEntry == jobEntryCopy && checkpointResult!=null) {

      // Yes, we need to restore the previous checkpoint results at this point...
      //
      checkpointResult.setResult(true);
      checkpointResult.setNrErrors(0);
      extension.result = checkpointResult; // will be used in the job to set result and prevResult

      // Do we need to restore the parameter values?
      //
      if ("Y".equalsIgnoreCase(job.environmentSubstitute(checkpointLogTable.getSaveParameters()))) {
        for (String name : checkpointParameters.keySet()) {
          job.setParameterValue(name, checkpointParameters.get(name));
        }
      }

      // Do we need to keep the result rows from the checkpoint?
      //
      if (!"Y".equalsIgnoreCase(job.environmentSubstitute(checkpointLogTable.getSaveResultRows()))) {
        checkpointResult.getRows().clear();
      }

      // Do we need to keep the result files from the checkpoint?
      //
      if (!"Y".equalsIgnoreCase(job.environmentSubstitute(checkpointLogTable.getSaveResultFiles()))) {
        checkpointResult.getResultFiles().clear();
      }

      // Add some basic results logging
      //
      JobEntryResult jerCheckpoint = new JobEntryResult(checkpointResult, job.getLogChannel().getLogChannelId(),
          BaseMessages.getString(PKG, "JobBeforeJobEntryExectionExtensionPointPlugin.Comment.JobRestartAtCheckpoint"), null, jobEntryCopy.getName(),
          jobEntryCopy.getNr(), job.environmentSubstitute(jobEntryCopy.getEntry().getFilename()));
      jerCheckpoint.setCheckpoint(true);
      job.getJobTracker().addJobTracker(new JobTracker(jobMeta, jerCheckpoint));
      synchronized (job.getJobEntryResults()) {
        job.getJobEntryResults().add(jerCheckpoint);
      }

      log.logBasic("Restarting from checkpoint job entry: " + checkpointJobEntry.toString());

    }

    
  }

}
