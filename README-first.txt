This repository plugin for Kettle makes web service calls to the Pentaho unified repository (PUR).

Building and Installing the Plugin
------------------------------------

The easiest way to build and deploy the plugin is to use the Ivy settings for the project. 
In the build.properties file, set the kettle.plugin.dir variable to the Kettle repositories plugin directory. 
Run the following targets in order: clean, resolve, dist, install

Project Directory Overview
-----------------------------
/bin  			All compiled and staging classes/resources end up here.
/build-res  	These are the core subfloor files, used as common build targets.
/classes 		Java classes get compiled to this directory. 
/dist			Two artifacts end up here - the jar for the repository classes, and the zip file that contains all resources for  
				the plugin.
/lib			When an ant 'resolve' target is run, this dir is populated with all necessary default conf libraries.
/libswt			The OS specific SWT jars needed for the UI pieces of the plugin. These today are not checked into an artifactory. 
/package-res	The configuration file for the plugin lives here. (plugin.xml) Any other files here are copied to the root of 
				the plugin distribution artifact. 
/src			The source code. 
/test			The JUnit test source code. 
/test-lib		When an ant 'resolve' target is run, this dir is populated with all necessary test conf libraries.
/testfiles		Resources needed by the JUnit tests.

*** BEGIN OLD DOC ***

Eclipse JackrabbitRepository
============================

Use the Ivy files and/or  IvyDe, then the trunk of kettle will be retrieved for you as part of the dependency list. 

How to runs the unit test...
------------------------------

I dropped Jackrabbit-1.5.6.war into Apache Tomcat (webapps dir), renamed to jackrabbit.war.
Add the jcr-1.0.jar, found bundled with the specification, to your Tomcat commons/lib directory.  
Changed the port on which tomcat runs to 8181
Started tomcat, went to http://localhost:8181/jackrabbit

Hit the "Create Content Repository" button.

You'll now be able to run the unit tests. (which should all pass at this time)

JCR Browser
------------

If you want to see how things are stored, versions, debug the JCR, download and install Eclipse plugin "JCR Browser":

http://sourceforge.net/projects/jcrbrowser/

Unzip in your eclipse plugins directory, restart Eclipse, Preferences / JCR Browser
- Select "Apache Jackrabbit repository via HTTP"
- Host : localhost
- Port : 8181/jackrabbit

Then you can connect to the repository described above and browse it.
 
*** END OLD DOC ***