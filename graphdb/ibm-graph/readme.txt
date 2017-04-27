==================
IBM Graph README
==================

OVERVIEW
--------

IBM Graph is a cloud based graph based on Titan 1.  More details about it are available here:

https://www.ibm.com/us-en/marketplace/graph

GETTING STARTED
---------------

In order to either test or use Atlas with IBM Graph, you need to create an IBM Graph Service instance in
BlueMix.

1) Log in to BlueMix.  See http://bluemix.net for information about to how to create an account and sign in.

2) You can either create an instance in the U.S. South or the United Kingdom region.  Go to the URL
corresponding to the region you want to use:

    U.S. South: https://console.ng.bluemix.net/catalog/services/ibm-graph/
    United Kingdom: https://console.eu-gb.bluemix.net/catalog/services/ibm-graph/

When you navigate to this page, use the blue "Create" button to create the service.

3) Locate the credentials for the service you created.

    - Click on the "Service Credentials" tab on the page that appears when you create the service.
    - In the list of Service Credentials that appears, click on the action "View Credentials"
    - When you click "View Credentials" you will see something that looks like the following:

{
  "apiURL": "https://ibmgraph-alpha.ng.bluemix.net/aaaaaaa0-bbbb-cccc-dddd-eeeeeeeeeeee/g",
  "username": "aaaaaaa1-bbbb-cccc-dddd-eeeeeeeeeeee",
  "password": "aaaaaaa2-bbbb-cccc-dddd-eeeeeeeeeeee"
}

This is the information you will need to use when you configure Atlas to use your service.

When configuring the Atlas, the apiURL you need to use is what the IBM Graph Documentation calls
the BASE_URL.  This is obtained by taking the URL from the credentials and removing the trailing /g.  For example:

https://ibmgraph-alpha.ng.bluemix.net/aaaaaaa0-bbbb-cccc-dddd-eeeeeeeeeee

BUILDING/TESTING WITH IBM GRAPH
-------------------------------

1) Add ibm.graph.graph.id, ibm.graph.api.url, ibm.graph.username, ibm.graph.password to your maven settings.xml.

This is only required if you want to run the Atlas tests against IBM Graph.

These properties should be set using the values from the service credentials described above, with ibm.graph.api.url
being set to the BASE_URL.  The ibm.graph.graph.id property identifies what graph will be used by default.  The
graph name can contain letters,numbers, periods, and underscore characters.  This graph will be created if it does
not already exist.

At this time, the credentials in need to be in clear text.  Additional work is needed to support having them
encrypted in your settings.xml.

2) Build an Atlas bundle configured for IBM Graph.

mvn install -P dist -P ibm-graph

This will build Atlas and run all of the tests with against ibm-graph.

Only Atlas builds that were generated with the ibm-graph maven profile enabled can be used to use Atlas with IBM Graph.

NOTE: You can generate a build without running the tests by adding the option "-DskipTests" to the maven command line:

mvn install -P dist -P ibm-graph -DskipTests


RUNNING ATLAS WITH IBM GRAPH
--------------------------

In order to use Atlas with IBM Graph, you need to obtain an Atlas distribution that was built with the ibm-graph
profile enabled.  See the section "BUILDING/TESTING ATLAS WITH IBM GRAPH" for details.


1) Unpack the Atlas distribution in the location where you want Atlas to be installed.

2) Update conf/credentials.json in the unpacked distribution with the BASE_URL, username, and password for the IBM
Graph service you want to use.  Set the graphName to the name of the graph you want Atlas to use when it starts.
The graph will be created if it does not exist.

At this time, the credentials in credentials.json need to be in clear text.  Additional work is needed to support
having them encrypted in the file.

3) Start Atlas.

When you start Atlas, it will take several minutes for the base types and indices to be created.  This is normal,
and a one-time operation.  Atlas will not respond to any http requests until the initialization process is complete.


KNOWN ISSUES
------------

1) The v2/lineage/{guid}, lineage/{guid}/inputs/graph, and lineage/{guid}/outputs/graph endpoints fail when using
IBM Graph

EntityLineageService is hard-coded to generate Gremlin that is specific to TinkerPop 2.  IBM Graph does not
understand TinkerPop 2 syntax.  It requires TinkerPop 3.  This service needs to be updated to use
GremlinExpressionFactory to generate Gremlin that is appropriate for the version of TinkerPop being used.

See https://issues.apache.org/jira/browse/ATLAS-1579


2) ExportService is hard-coded to generate Gremlin that is specific to TinkerPop 2.  It needs to be updated to use
GremlinExpressionFactory to generate Gremlin that is appropriate for the version of TinkerPop being used.

See https://issues.apache.org/jira/browse/ATLAS-1588

3) Any operations which require catalog queries fail.  This causes an error dialog to appear in the dashboard
when performing certain operations which says:

An unexpected error has occurred.
   org.apache.atlas.catalog.exception.CatalogRuntimeException:
   java.lang.IllegalArgumentException:
   Could not find implementation class: ignored

The issue here is that BaseQuery and its subclasses in the catalog project are hard-coded to generate Gremlin that
is specific to TinkerPop 2.  It needs to be updated to use GremlinExpressionFactory to generate Gremlin that is
appropriate for the version of Titan being used.  In addition, it has direct dependencies on titan 0 / TinkerPop 2
classes.  As a result, the catalog functionality does not work when using IBM Graph.  Furthermore, as it exists now,
catalog has a direct dependency on TitanGraph.

See https://issues.apache.org/jira/browse/ATLAS-1580
