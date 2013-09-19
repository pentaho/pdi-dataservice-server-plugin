null
package com.pentaho.di.job;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseTransactionListener;
import org.pentaho.di.core.database.map.DatabaseConnectionMap;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobAdapter;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;

@ExtensionPoint(
    id="JobTransactionExtensionPlugin",
    extensionPointId="JobStart",
    description="Generates unique transaction ID and closes unique connection at the end of the job"    
   )
public class JobTransactionExtensionPlugin implements ExtensionPointInterface {
  private static Class<?> PKG = JobTransactionExtensionPlugin.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof Job)) {
      return;
    }
    
    Job job = (Job) object;
    JobMeta jobMeta = job.getJobMeta();
    
    boolean uniqueConnections = "Y".equalsIgnoreCase(jobMeta.getAttribute(JobRestartConst.JOB_RESTART_GROUP, 
        JobRestartConst.JOB_DIALOG_ATTRIBUTE_UNIQUE_CONNECTIONS));
    
    if (!uniqueConnections) {
      return;
    }

    final Job parentJob = job.getParentJob();
    final boolean parentUniqueConnections = ( parentJob!=null && 
        "Y".equalsIgnoreCase(parentJob.getJobMeta().getAttribute(
            JobRestartConst.JOB_RESTART_GROUP, 
            JobRestartConst.JOB_DIALOG_ATTRIBUTE_UNIQUE_CONNECTIONS
           ))
          ) || job.getParentTrans()!=null && job.getParentTrans().getTransMeta().isUsingUniqueConnections()
        ;
    
    job.setTransactionId(calculateTransactionId(job, parentJob, parentUniqueConnections));
    
    // If the job is running database transactional with unique connections, close them at the end...
    //
    job.addJobListener(new JobAdapter() {
      @Override
      public void jobFinished(Job job) throws KettleException {
        closeUniqueDatabaseConnections(job, parentJob, parentUniqueConnections);
      }
    });
  }
  
  /**
   * Calculates and returns the transaction id.  What is returned is:<br/>
   * <ul>
   *    <li>If there is a parent job using unique connections, return parent job's transaction id.  Otherwise....</li>
   *    <li>If the parent logging object is a transformation using unique connections return the transformation's transactio id.  Otherwise...</li>
   *    <li>The next available transaction id from the Database Connection Map.</li>
   * </ul>
   * @param parentUniqueConnections 
   * @param parentJob 
   * @param job 
   *   
   * @return String transaction id
   */
  public String calculateTransactionId(Job job, Job parentJob, boolean parentUniqueConnections) {
    
    if (parentJob != null && parentUniqueConnections) {
      return parentJob.getTransactionId();
    } else if (job.getParentLoggingObject() != null && (job.getParentLoggingObject() instanceof Trans)
        && ((Trans) job.getParentLoggingObject()).getTransMeta().isUsingUniqueConnections()) {
      return ((Trans) job.getParentLoggingObject()).getTransactionId();
    } else {
      return DatabaseConnectionMap.getInstance().getNextTransactionId();
    }
  }

  /**
   * Closes database connections.  Database connections that have transactions in use by the parent job are not closed.
   */
  protected void closeUniqueDatabaseConnections(final Job job, Job parentJob, boolean parentUniqueConnections) {
    
    // Don't close any connections if the parent job is using the same transaction
    // 
    if (parentUniqueConnections && 
        parentJob != null && job.getTransactionId() != null && parentJob.getTransactionId() != null
        && job.getTransactionId().equals(parentJob.getTransactionId())) {
      return;
    }

    // Don't close any connections if the parent transformation is using the same transaction
    // 
    if (job.getParentTrans()!=null && job.getParentTrans().getTransMeta().isUsingUniqueConnections() && 
        job.getTransactionId() != null && job.getParentTrans().getTransactionId() != null &&
        job.getTransactionId().equals(job.getParentTrans().getTransactionId())) {
      return;
    }

    // First we get all the database connections ...
    //
    DatabaseConnectionMap map = DatabaseConnectionMap.getInstance();
    synchronized (map) {
      List<Database> databaseList = new ArrayList<Database>(map.getMap().values());
      for (Database database : databaseList) {
        if (database.getConnectionGroup().equals(job.getTransactionId())) {
          try {

            // This database connection belongs to this transformation.
            // Let's roll it back if there is an error...
            //
            if (job.getResult().getNrErrors() > 0) {
              try {
                database.rollback(true);

                job.getLogChannel().logBasic(BaseMessages.getString(PKG, "Job.Exception.TransactionsRolledBackOnConnection",
                    database.toString()));
              } catch (Exception e) {
                throw new KettleDatabaseException(BaseMessages.getString(PKG,
                    "Job.Exception.ErrorRollingBackUniqueConnection", database.toString()), e);
              }
            } else {
              try {
                database.commit(true);

                job.getLogChannel().logBasic(BaseMessages.getString(PKG, "Job.Exception.TransactionsCommittedOnConnection",
                    database.toString()));
              } catch (Exception e) {
                throw new KettleDatabaseException(BaseMessages.getString(PKG,
                    "Job.Exception.ErrorCommittingUniqueConnection", database.toString()), e);
              }
            }
          } catch (Exception e) {
            job.getLogChannel().logError(BaseMessages.getString(PKG, "Job.Exception.ErrorHandlingJobTransaction", database.toString()),
                e);
            job.getResult().setNrErrors(job.getResult().getNrErrors() + 1);
          } finally {
            try {
              // This database connection belongs to this transformation.
              database.closeConnectionOnly();
            } catch (Exception e) {
              job.getLogChannel().logError(
                  BaseMessages.getString(PKG, "Job.Exception.ErrorHandlingJobTransaction", database.toString()), e);
              job.getResult().setNrErrors(job.getResult().getNrErrors() + 1);
            } finally {
              // Remove the database from the list...
              //
              map.removeConnection(database.getConnectionGroup(), database.getPartitionId(), database);

              // Remove the listeners
              //
              map.removeTransactionListeners(job.getTransactionId());
            }
          }
        }
      }

      // Who else needs to be informed of the rollback or commit?
      //
      List<DatabaseTransactionListener> transactionListeners = map.getTransactionListeners(job.getTransactionId());
      if (job.getResult().getNrErrors() > 0) {
        for (DatabaseTransactionListener listener : transactionListeners) {
          try {
            listener.rollback();
          } catch (Exception e) {
            job.getLogChannel().logError(BaseMessages.getString(PKG, "Job.Exception.ErrorHandlingTransactionListenerRollback"), e);
            job.getResult().setNrErrors(job.getResult().getNrErrors() + 1);
          }
        }
      } else {
        for (DatabaseTransactionListener listener : transactionListeners) {
          try {
            listener.commit();
          } catch (Exception e) {
            job.getLogChannel().logError(BaseMessages.getString(PKG, "Job.Exception.ErrorHandlingTransactionListenerCommit"), e);
            job.getResult().setNrErrors(job.getResult().getNrErrors() + 1);
          }
        }
      }
    }
  }
  
}
