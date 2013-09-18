package com.pentaho.di.job;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogTableField;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobAdapter;
import org.pentaho.di.job.JobExecutionExtension;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.w3c.dom.Node;

@ExtensionPoint(
    extensionPointId="JobBeginProcessing", 
    id="JobBeginProcessingExtensionPointPlugin", 
    description="Look up checkpoint information and much more during the beginning of the processing of a job"
  )
public class JobBeginProcessingExtensionPointPlugin implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    
    if (!(object instanceof JobExecutionExtension)) return;
    
    JobExecutionExtension extension = (JobExecutionExtension) object;

    final Job job = extension.job;
    final JobMeta jobMeta = job.getJobMeta();
    final CheckpointLogTable checkpointLogTable = JobRestartConst.getCheckpointLogTable(jobMeta);
    
    // See if we need to start at an alternate location in the job based on the information from the run ID
    //
    if (checkpointLogTable!=null && checkpointLogTable.isDefined()) {
      // Look up information from the previous attempt
      //
      lookupCheckpoint(job, checkpointLogTable);

      final int runAttemptNr = JobRestartConst.getCheckpointAttemptNr(job);
      final JobEntryCopy checkpointJobEntry = JobRestartConst.getCheckpointJobEntry(job);
      final Result checkpointResult = JobRestartConst.getCheckpointResult(job);
      job.setStartJobEntryResult(checkpointResult);
      
      // Make sure to write the attempt information
      //
      if (runAttemptNr > 1) {
        JobRestartConst.writeCheckpointInformation(checkpointLogTable, job, checkpointJobEntry, checkpointResult);
      }

      // At the end of the job, if everything ran without error, clear the job entry name from the checkpoint table.
      // That way we know next time around we can grab a new run ID for this job.
      //
      job.addJobListener(new JobAdapter() {
        @Override
        public void jobFinished(Job job) throws KettleException {
          // Clear the checkpointRun information in the checkpoint table
          // But only in case everything ran without error in the job.
          //
          if (job.getResult() != null && job.getResult().getResult() && job.getResult().getNrErrors() == 0) {
            JobRestartConst.setCheckpointJobEntry(job, null);
            JobRestartConst.writeCheckpointInformation(checkpointLogTable, job, null, job.getResult());
          }
        }
      });
    }
  }
  
  
  
  /**
   * Looks up checkpoint information and updates the Job if appropriate.
   * @param job the job
   * @param logTable the checkpoint log table
   * @throws KettleException
   */
  protected void lookupCheckpoint(final Job job, final CheckpointLogTable logTable) throws KettleException {

    final JobMeta jobMeta = job.getJobMeta();
    
    try {
      // Set some defaults regardless of lookup...
      //
      JobRestartConst.setCheckpointRunId(job, -1);

      // Take start of this transformation if we have nothing else.
      // This will be logged by the checkpoints later on.
      //
      JobRestartConst.setCheckpointRunStartDate(job, job.getLogDate());

      // No job entry to restart from by default: use the Start job entry
      //
      JobRestartConst.setCheckpointJobEntry(job, null);

      // This is the first attempt by default
      //
      JobRestartConst.setCheckpointAttemptNr(job, 1);

      // If we don't want to use a previous checkpoint, pretend we didn't find it.
      //
      if (JobRestartConst.isIgnoringCheckpoints(job)) { 
        return;
      }
      
      // Now look up some data in the check point log table...
      //
      String namespace = Const.NVL(job.getParameterValue(logTable.getNamespaceParameter()), "-");
      DatabaseMeta dbMeta = logTable.getDatabaseMeta();
      String schemaTable = dbMeta.getQuotedSchemaTableCombination(logTable.getActualSchemaName(),
          logTable.getActualTableName());
      Database db = null;
      try {
        db = new Database(job, dbMeta);
        db.shareVariablesWith(job);
        db.connect();
        db.setCommit(10);

        // The fields to retrieve
        //
        LogTableField idJobRunField = logTable.getKeyField();
        String idJobRunFieldName = dbMeta.quoteField(idJobRunField.getFieldName());
        LogTableField jobRunStartDateField = logTable.getJobRunStartDateField();
        String jobRunStartDateFieldName = dbMeta.quoteField(jobRunStartDateField.getFieldName());
        LogTableField checkpointNameField = logTable.getCheckpointNameField();
        String checkpointNameFieldName = dbMeta.quoteField(checkpointNameField.getFieldName());
        LogTableField checkpointNrField = logTable.getCheckpointCopyNrField();
        String checkpointNrFieldName = dbMeta.quoteField(checkpointNrField.getFieldName());
        LogTableField attemptNrField = logTable.getAttemptNrField();
        String attemptNrFieldName = dbMeta.quoteField(attemptNrField.getFieldName());
        LogTableField resultXmlField = logTable.getResultXmlField();
        String resultXmlFieldName = dbMeta.quoteField(resultXmlField.getFieldName());
        LogTableField parameterXmlField = logTable.getParameterXmlField();
        String parameterXmlFieldName = dbMeta.quoteField(parameterXmlField.getFieldName());

        // The parameters values to pass
        //
        RowMetaAndData pars = new RowMetaAndData();

        LogTableField jobNameField = logTable.getNameField();
        String jobNameFieldName = dbMeta.quoteField(jobNameField.getFieldName());
        pars.addValue(idJobRunFieldName, ValueMetaInterface.TYPE_STRING, jobMeta.getName());
        LogTableField namespaceField = logTable.getNamespaceField();
        String namespaceFieldName = dbMeta.quoteField(namespaceField.getFieldName());
        pars.addValue(namespaceFieldName, ValueMetaInterface.TYPE_STRING, namespace);

        String sql = "SELECT " + idJobRunFieldName + ", " + jobRunStartDateFieldName + ", " + checkpointNameFieldName
            + ", " + checkpointNrFieldName + ", " + attemptNrFieldName + ", " + resultXmlFieldName + ", "
            + parameterXmlFieldName;
        sql += " FROM " + schemaTable;
        sql += " WHERE " + jobNameFieldName + " = ? AND " + namespaceFieldName + " = ? ";
        sql += " AND " + checkpointNrFieldName + " IS NOT NULL"; // nulled at the successful end of the job so we don't need these rows.

        // Grab the matching rows, if more than one matches just grab the first...
        //
        PreparedStatement statement = db.prepareSQL(sql);
        ResultSet resultSet = db.openQuery(statement, pars.getRowMeta(), pars.getData());
        Object[] rowData = db.getRow(resultSet);

        // If there is no checkpoint found, call it a day
        //
        if (rowData == null) {
          return;
        }
        RowMetaInterface rowMeta = db.getReturnRowMeta();

        // Get the data from the row
        //
        int index = 0;
        Long lookupRunId = rowMeta.getInteger(rowData, index++);
        Date jobRunStartDate = rowMeta.getDate(rowData, index++);
        String checkpointName = rowMeta.getString(rowData, index++);
        Long checkpointNr = rowMeta.getInteger(rowData, index++);
        Long attemptNr = rowMeta.getInteger(rowData, index++);
        String resultXml = rowMeta.getString(rowData, index++);
        String parameterXml = rowMeta.getString(rowData, index++);

        // Do some basic checks on the table data...
        //
        if (lookupRunId == null || jobRunStartDate == null || checkpointName == null || checkpointNr == null
            || attemptNr == null || resultXml == null || parameterXml == null) {
          // Nothing to checkpoint to, call it quits.
          //
          return;
        }

        // See if the retry period hasn't expired...
        //
        Date runStartDate = JobRestartConst.getCheckpointRunStartDate(job);
        int retryPeriodInMinutes = Const.toInt(job.environmentSubstitute(logTable.getRunRetryPeriod()), -1);
        if (retryPeriodInMinutes > 0) {
          long maxTime = runStartDate.getTime() + retryPeriodInMinutes * 60 * 1000;
          if (job.getStartDate().getTime() > maxTime) {
            // retry period expired
            throw new KettleException("Retry period exceeded, please reset job [" + jobMeta.getName()
                + "] for namespace [" + namespace + "]");
          }
        }

        // Verify the max number of retries / attempts...
        //
        int maxAttempts = Const.toInt(job.environmentSubstitute(logTable.getMaxNrRetries()), -1);
        if (maxAttempts > 0) {
          if (attemptNr + 1 > maxAttempts) {
            throw new KettleException("The job checkpoint system has reached the maximum number or retries after "
                + attemptNr + " attempts");
          }
        }

        // All OK, now pass the looked up data to the job
        //
        JobRestartConst.setCheckpointRunStartDate(job, jobRunStartDate);
        JobRestartConst.setCheckpointRunId(job, lookupRunId);
        JobRestartConst.setCheckpointAttemptNr(job, attemptNr.intValue() + 1 );
        JobEntryCopy checkpointJobEntry = jobMeta.findJobEntry(checkpointName, checkpointNr.intValue(), false);
        JobRestartConst.setCheckpointJobEntry(job, checkpointJobEntry);
        if (checkpointJobEntry == null) {
          throw new KettleException("Unable to find checkpoint job entry with name [" + checkpointName
              + "] and copy number [" + checkpointNr + "]");
        }

        // We start at the checkpoint job entry (this is skipped though)
        //
        job.setStartJobEntryCopy(checkpointJobEntry);

        // The result
        //
        Result checkpointResult = new Result(XMLHandler.loadXMLString(resultXml, Result.XML_TAG));
        JobRestartConst.setCheckpointResult(job, checkpointResult);
        JobRestartConst.setCheckpointParameters(job, extractParameters(parameterXml));
      } catch (Exception e) {
        throw new KettleException("Unable to look up checkpoint information in the check point log table", e);
      } finally {
        if (db != null) {
          db.disconnect();
        }
      }
    } finally {
      // Set variables
      //
      long runId = JobRestartConst.getCheckpointRunId(job);
      job.setVariable(Const.INTERNAL_VARIABLE_JOB_RUN_ID, Long.toString(runId));
      int runAttemptNr = JobRestartConst.getCheckpointAttemptNr(job);
      job.setVariable(Const.INTERNAL_VARIABLE_JOB_RUN_ATTEMPTNR, Long.toString(runAttemptNr));
    }
  }




  /**
   * Extracts the parameters from the passed XML.
   * @param parameterXml
   * @return Map<String, String> 
   * @throws KettleException
   */
  private Map<String, String> extractParameters(String parameterXml) throws KettleException {
    Map<String, String> map = new HashMap<String, String>();
    List<Node> parameterNodes = XMLHandler.getNodes(XMLHandler.loadXMLString(parameterXml, "pars"), "par");
    for (Node parameterNode : parameterNodes) {
      String name = XMLHandler.getTagValue(parameterNode, "name");
      String value = XMLHandler.getTagValue(parameterNode, "value");
      map.put(name, value);
    }
    return map;
  }
}
