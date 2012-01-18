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
      
  	  -keep class org.pentaho.di.ui.repository.pur.PurRepositoryDialog {
    		  public protected *;
      }

      -keep class org.pentaho.di.ui.repository.EESpoonPlugin {}
	  -keep class org.pentaho.di.ui.repository.pur.repositoryexplorer.abs.controller.* {
  		  public protected *;
      }
      -keepnames class org.pentaho.di.repository.pur.PurRepositoryLocation
