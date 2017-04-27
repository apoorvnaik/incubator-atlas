The multi-tenancy support present here was originally implemented on a private Atlas fork.   We have preserved
the multi-tenancy support from that implementation in the ibm-graph project, but additional work is needed to
wire it up with the rest of Atlas.  This document describes the architecture and design of that multi-tenancy
implementation

In the original implementation, the tenant id to use came in as a http header.  In the AuditFilter, we set the
tenant id in the RequestContext, making it available to all of the downstream code processing the request.
IBMGraphDatabase used the tenant id from the RequestContext, along with the TenantGraphStrategy, to determine what
instance of IBMGraphGraph to return to callers of getUserGraph() and getSharedGraph().  There was a tenant id to
IBMGraphGraph map in IBMGraphDatabase that caches these instances.

Two different approaches were used for tenant data: graph-per-tenant and pertition-per-tenant.  With
graph-per-tenant, each tenant would have its own graph instance within IBM Graph.  We mapped the tenant id directly
to the name of the graph within IBM Graph to use to service that tenant.  If the graph did not exist, we would
create it on demand.  At that point, all of the base Atlas types and indices would be created within the new graph.
To support this model, the notion of an ITenantRegistrationListener was introduced.  This interface provided the
abstraction layer with a callback it could use to trigger the creation of the base types and indices without adding
a compile-time dependency on the repository project.  A default implementation of the interface was added to the
repository project with the logic to do this initialization.  All of the logic specific to the  graph-per-tenant
approach is encapsulated in org.apache.atlas.ibmgraph.GraphPerTenantStrategy.  A major disadvantage of this approach
is that we found that the time to create a new tenant was very costly.  In response to that, a second model was
developed.

With partition-per-tenant, data for all tenants is stored in a single graph, and each tenant has a separate
partition within that graph. The partitioned graph strategy built into TinkerPop 3 is used to isolate the tenants.
Basically, there is a property added to all vertices that indicates what tenant the vertex belongs to.  This
property is used when running gremlin queries.  A condition to restrict the query results to the correct tenant is
added implicitly by the PartionedGraphStaregy.  In partition-per-tenant, there is no need for the on-demand type
registration mechanism.  The Atlas server starts up the normal way, creating the base types and indices the first
time it starts.  All of the logic specific to partition-per-tenant is encapsulated in
org.apache.atlas.ibmgraph.PartitionPerTenantStrategy.

The graph-per-tenant and partition-per-tenant data storage implementations are included in this project, but at this
writing, Atlas core has not been modified to enable multi-tenancy.  The ITenantRegistrationListener has been moved,
for now, into the ibm-graph project.  No implementations have been created for it.  The original implementation was
based on a code path that no longer exists in the public Atlas.

There is a third TenantGraphStrategy that was created which disables multi-tenancy:
org.apache.atlas.ibmgraph.MultiTenancyDisabledStrategy.  This is the strategy that is being used now.

The tenant data storage strategy is configured via the atlas.graphdb.tenantGraphStrategyImpl property in
atlas-application.properties.  Currently, only org.apache.atlas.ibmgraph.MultiTenancyDisabledStrategy can be used.

The multi-tenancy implementation also introduced the notion of "shared" and "user" graphs.  The shared graph is used
when there is no tenant and when persisting type definitions, and the user graph is used for storing
tenant-specific data.

Our general approach for contributing the IBM Graph implementation to the public Atlas was that we wanted to leave
as much of the multi-tenancy implementation in place as possible so it could be easily hooked up as part of a more
complete multi-tenancy solution.   Although the methods to get the "shared" and "user" graph in GraphDatabase
are in place, the Atlas code base has not been updated to adopt them.  In the original implementation,
AtlasGraphProvider.getSharedGraph() and AtlasGraphProvider.getUserGraph() were added and used by the rest of Atlas
to get the appropriate graph to use.  These methods have *not* been added at this point.  There is still a single
AtlasGraphProvider.getGraphInstance() which always uses the "user" graph.  To enable multi-tenancy in Atlas with IBM
Graph, the Atlas core code requires changes to use the correct graph (user or shared).  Specifically, the logic for
persisting type defintions needs to be updated to use the shared graph instance, and the logic for creating and
querying for entities needs to be updated to use the user graph.  Additionally, AtlasGraphProvider had logic to call
a new method GraphDatabase.registerListener() to configure the implementation of ITenantRegistrationListener to use.
That logic also has not been added.

