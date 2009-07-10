
Eclipse JackrabbitRepository
============================

Has a dependency to "Kettle trunk" at this time, will remove that later.
In the mean time, import the project into your Eclipse.
Either that or drop new kettle*.jar files in libext AND most of the rest of Kettle trunk/libext as well.

How to runs the unit test...
------------------------------

I dropped Jackrabbit-1.5.6.war into Apache Tomcat (webapps dir), renamed to jackrabbit.war.
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

