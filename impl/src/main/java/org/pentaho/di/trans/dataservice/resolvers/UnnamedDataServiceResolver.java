package org.pentaho.di.trans.dataservice.resolvers;

import com.google.common.base.Function;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceContext;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.DataServiceExecutor.Builder;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.IDataServiceMetaFactory;
import org.pentaho.di.trans.step.StepMeta;

import java.util.List;

public class UnnamedDataServiceResolver implements DataServiceResolver {

  private IDataServiceMetaFactory dataServiceMetaFactory;
  private DataServiceContext context;

  public UnnamedDataServiceResolver( IDataServiceMetaFactory dataServiceMetaFactory, DataServiceContext context ) {
    this.dataServiceMetaFactory = dataServiceMetaFactory;
    this.context = context;
  }

  @Override
  public DataServiceMeta getDataService( String dataServiceName ) {
    return null;
  }

  @Override
  public List<DataServiceMeta> getDataServices( Function<Exception, Void> logger ) {
    return null;
  }

  @Override
  public List<DataServiceMeta> getDataServices( String dataServiceName, Function<Exception, Void> logger ) {
    return null;
  }

  @Override
  public List<String> getDataServiceNames() {
    return null;
  }

  @Override
  public List<String> getDataServiceNames( String dataServiceName ) {
    return null;
  }

  @Override
  public Builder createBuilder( SQL sql ) throws KettleException {
    String dataserviceName = sql.getServiceName();
    String ktrPath = "/public/plugin-samples/pentaho-cdf-dd/legacy/realtime";
    String ktrName = "real_time";
    String stepName = "Data generation";

    String repoName = "Pentaho";
    String username = "admin";
    String password = "password";

    // Connecting to the repository
    RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
    repositoriesMeta.readData();
    RepositoryMeta repositoryMeta = repositoriesMeta.findRepository( repoName );
    PluginRegistry registry = PluginRegistry.getInstance();
    Repository repository = registry.loadClass(
        RepositoryPluginType.class,
        repositoryMeta,
        Repository.class
    );
    repository.init(repositoryMeta);

    repository.connect( username, password );

    // Find the directory
    RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
    RepositoryDirectoryInterface fooBar = tree.findDirectory( ktrPath );

    // Load the transformation
    TransMeta transMeta = repository.loadTransformation( ktrName, fooBar, null, true, null );
    StepMeta stepMeta = transMeta.findStep( stepName );

    DataServiceMeta dataServiceMeta = this.dataServiceMetaFactory.createDataService( stepMeta );
    if ( dataServiceMeta != null ) {
      dataServiceMeta.setName( dataserviceName );
      dataServiceMeta.setStreaming( true );
      dataServiceMeta.setRowLimit( 1000 );
      dataServiceMeta.setTimeLimit( 10000 );
      if ( dataServiceMeta.isStreaming() ) {
        return new DataServiceExecutor.Builder( sql, dataServiceMeta, context )
            .rowLimit( dataServiceMeta.getRowLimit() ).timeLimit( dataServiceMeta.getTimeLimit() );
      }
      return new DataServiceExecutor.Builder( sql, dataServiceMeta, context );
    }
    return null;
  }
}
