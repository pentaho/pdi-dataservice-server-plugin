	  -keepclasseswithmembernames class * implements org.pentaho.di.repository.Repository {
		  public protected *;
	  }
  	  -keepclasseswithmembernames class * implements org.pentaho.di.repository.RepositorySecurityProvider {
  		  public protected *;
  	  }
	  -keepclasseswithmembernames class * implements ObjectRevision {
		  public protected *;
	  }
	  -keepclasseswithmembernames class * implements org.pentaho.di.repository.RepositoryMeta {
		  public protected *;
	  }
	  -keepclasseswithmembernames class * implements org.pentaho.di.repository.RepositorySecurityProvider {
		  public protected *;
	  }
  	  -keepclasseswithmembernames class * implements org.pentaho.di.repository.RepositorySecurityProvider {
  		  public protected *;
  	  }
  	  -keepclasseswithmembernames class * implements  org.pentaho.di.repository.RepositoryVersionRegistry {
  		  public protected *;
  	  }
      -keepclasseswithmembernames class * implements org.pentaho.ui.xul.impl.AbstractXulEventHandler {
      	public protected *;	
      }
      
      -keep class * implements org.pentaho.ui.xul.XulEventSourceAdapter {
          void set*(***);
          void set*(int, ***);

          boolean is*(); 
          boolean is*(int);

          *** get*();
          *** get*(int);
      }
      
      -keep class org.pentaho.di.repository.pur.PurRepositoryLocation {
        public *;
      }

  	  -keep class org.pentaho.di.ui.repository.pur.PurRepositoryDialog {
    		  public protected *;
      }
      
      -keep class org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.ConnectionPermissionsController {
    		  void apply();
      }

      -keep class org.pentaho.di.ui.repository.pur.repositoryexplorer.controller.PermissionsController {
                    public *;
      }

      -keep class org.pentaho.di.ui.repository.EESpoonPlugin {}
	  -keep class org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.* {
  		  public protected *;
      }
      -keep class com.pentaho.di.job.CheckpointLogTable {}
      -keep class com.pentaho.di.job.CheckpointLogTableUserInterface {}
      -keep class com.pentaho.di.job.JobRestart {}
      -keep class com.pentaho.di.trans.dataservice.DataServiceTransDialogTab {}
      -keep class com.pentaho.di.services.PentahoDiPlugin {
        public *;
      }

      -keep class com.pentaho.di.purge.* {
        public *;
      }

      -keep class com.pentaho.di.revision.* {
        public *;
      }

      -keepclassmembers class org.pentaho.di.repository.pur.PurObjectRevision {
        <fields>;
      }
      
      -keepnames class org.pentaho.di.repository.pur.PurRepositoryLocation
      
      -keep class com.pentaho.repository.importexport.* {
			  public static <methods>;
		  }

	-adaptresourcefilenames		**.properties

