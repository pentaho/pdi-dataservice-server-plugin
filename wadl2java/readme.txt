Tim Kafalas 8/6/2014

The org.pentaho.di.services.PentahoDiPlugin class it generated completely from our wadl file and the wadl2java
ant task in the build-wadl2java.xml file.  Below are the steps to regenerating the class if changes are made to
the plugin's rest services.

1) Generate the new wadl file by issuing the following service request to a running pdi server with your rest
   service changes: 
   
   			http://localhost:9080/pentaho-di/plugin/pur-repository-plugin/api/application.wadl

2) Overwrite the wadl2java/wadl-resource/application.wadl.xml file with the results of step 1.

3) Run the "wadl2java-resolve" task in the build.xml file to retrieve all the dependencies needed
	 to run wadl2java. This task will generate a populated wadl2java/lib folder.
	 
4) Run the "wadl2java" ant task on the build-wadl2java.xml file.  This task was put in a separate file because
	 the build file would not compile if the class defined in the <taskdef> node was not present.