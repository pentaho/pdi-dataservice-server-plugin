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

import java.util.Date;
import java.util.Map;

import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogStatus;
import org.pentaho.di.core.logging.LogTableInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;

public class JobRestartConst {
  private static Class<?> PKG = JobRestartConst.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  public static final String JOB_RESTART_GROUP = "JobRestart";
  public static final String JOB_DIALOG_ATTRIBUTE_UNIQUE_CONNECTIONS = "UniqueConnections";
  public static final String JOB_ENTRY_CHECKPOINT_MARK = "CheckpointMark";

  public static final String DATA_CHECKPOINT_ENTRY = "CHECKPOINT_ENTRY";
  public static final String DATA_CHECKPOINT_RESULT = "CHECKPOINT_RESULT";
  public static final String DATA_CHECKPOINT_PARAMETERS = "CHECKPOINT_PARAMETERS";
  public static final String DATA_CHECKPOINT_IGNORE = "CHECKPOINT_IGNORE";
  public static final String DATA_CHECKPOINT_RUN_ID = "CHECKPOINT_RUN_ID";
  public static final String DATA_CHECKPOINT_ATTEMPT_NR = "CHECKPOINT_ATTEMPT_NR";
  public static final String DATA_CHECKPOINT_RUN_START_DATE = "CHECKPOINT_RUN_DATE";
  
  public static final String DATA_JOBENTRY_IS_CHECKPOINT = "IS_CHECKPOINT"; 
  
  public static final String OPTION_IGNORE_CHECKPOINTS = "IgnoreCheckpoints"; 
  
  
  
  public static JobEntryCopy getCheckpointJobEntry(Job job) {
    return (JobEntryCopy) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_ENTRY);
  }
  
  public static void setCheckpointJobEntry(Job job, JobEntryCopy checkpointJobEntry) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_ENTRY, checkpointJobEntry);
  }

  public static Result getCheckpointResult(Job job) {
    return (Result) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_RESULT);
  }

  public static void setCheckpointResult(Job job, Result checkpointResult) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_RESULT, checkpointResult);
  }
  
  public static long getCheckpointRunId(Job job) {
    Long runId = (Long) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_RUN_ID);
    if (runId==null) return -1;
    return runId.longValue();
  }

  public static void setCheckpointRunId(Job job, long runId) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_RUN_ID, runId);
  }

  public static int getCheckpointAttemptNr(Job job) {
    Integer attemptNr = (Integer) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_ATTEMPT_NR);
    if (attemptNr==null) return 0;
    return attemptNr.intValue();
  }

  public static void setCheckpointAttemptNr(Job job, int attemptNr) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_ATTEMPT_NR, attemptNr);
  }

  public static void setCheckpointRunStartDate(Job job, Date runStartDate) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_RUN_START_DATE, runStartDate);
  }

  public static Date getCheckpointRunStartDate(Job job) {
    return (Date) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_RUN_START_DATE);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> getCheckpointParameters(Job job) {
    return (Map<String, String>) job.getExtensionDataMap().get(JobRestartConst.DATA_CHECKPOINT_PARAMETERS);
  }

  public static void setCheckpointParameters(Job job, Map<String, String> checkpointParameters) {
    job.getExtensionDataMap().put(JobRestartConst.DATA_CHECKPOINT_PARAMETERS, checkpointParameters);
  }

  public static CheckpointLogTable getCheckpointLogTable(JobMeta jobMeta) {
    for (LogTableInterface logTable : jobMeta.getExtraLogTables()) {
      if (logTable instanceof CheckpointLogTable) {
        return (CheckpointLogTable) logTable;
      }
    }
    return null;
  }
  
  public static void setCheckpointLogTable(JobMeta jobMeta, CheckpointLogTable checkpointLogTable) {
    int index=-1;
    for (int i=0;i<jobMeta.getExtraLogTables().size();i++) {
      if (jobMeta.getExtraLogTables().get(i) instanceof CheckpointLogTable) {
        index=i;
        break;
      }
    }
    if (index>=0) {
      jobMeta.getExtraLogTables().set(index, checkpointLogTable);
    } else {
      jobMeta.getExtraLogTables().add(checkpointLogTable);
    }
  }

  public static boolean isCheckpoint(JobEntryCopy jobEntryCopy) {
    String cp = jobEntryCopy.getAttribute(JobRestartConst.JOB_RESTART_GROUP, JobRestartConst.JOB_ENTRY_CHECKPOINT_MARK);
    return cp!=null && "Y".equalsIgnoreCase(cp);
  }  
  
  public static void setCheckpoint(JobEntryCopy jobEntryCopy, boolean isCheckpoint) {
    jobEntryCopy.setAttribute(JobRestartConst.JOB_RESTART_GROUP, JobRestartConst.JOB_ENTRY_CHECKPOINT_MARK, isCheckpoint?"Y":"N");
  }
  
  /**
   * Writes a checkpoint to the checkpoint log table.
   * @param jobEntryCopy
   * @param result
   * @throws KettleException
   */
  public static void writeCheckpointInformation(CheckpointLogTable checkpointLogTable, Job job, JobEntryCopy jobEntryCopy, Result result) throws KettleException {
    Database db = null;

    // Keep track in the job itself so everyone knows about this
    //
    setCheckpointJobEntry(job, jobEntryCopy);

    try {
      db = new Database(job, checkpointLogTable.getDatabaseMeta());
      db.shareVariablesWith(job);
      db.connect();
      db.setCommit(10);

      LogStatus logStatus = LogStatus.RUNNING;
      
      long runId = getCheckpointRunId(job);

      // First run ever, look up a new value for the run ID.
      //
      if (runId < 0) {
        runId = checkpointLogTable.getDatabaseMeta().getNextBatchId(db, checkpointLogTable.getSchemaName(),
            checkpointLogTable.getTableName(), checkpointLogTable.getKeyField().getFieldName());

        // This is the first attempt to run this job
        // 
        logStatus = LogStatus.START;
        JobRestartConst.setCheckpointRunId(job, runId);
      }

      db.writeLogRecord(checkpointLogTable, logStatus, result, job);

      // Also time-out the log records in here...
      //
      db.cleanupLogRecords(checkpointLogTable);

    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "JobRestartConst.Exception.UnableToWriteCheckpointInformationToLogTable"), e);
    } finally {
      if (!db.isAutoCommit())
        db.commit(true);
      db.disconnect();
    }
  }

  public static boolean isIgnoringCheckpoints(Job job) {
    String ignore = (String)job.getExtensionDataMap().get(OPTION_IGNORE_CHECKPOINTS);
    
    return "Y".equalsIgnoreCase(ignore) || "YES".equalsIgnoreCase(ignore) || "TRUE".equalsIgnoreCase(ignore);
  }
}
