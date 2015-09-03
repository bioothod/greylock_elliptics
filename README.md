# Greylock

A scalable way to built search engine.

Greylock is a rather simple C++ search engine which would take off the burden of the engine configuration,
and yet be able to scale from hundreds to billions of indexed documents
and from single to hundreds servers with as little as possible operations intervention.

Having constant or increasing rate of the input documents we at [Reverbrain](http://reverbrain.com/)
wanted to spend as little as possible on search engine resharding, moving data around,
constantly looking at shards load and so on. We just wanted to add new servers
when needed, and optionally running mundane operational tasks in background
(better started automatically by some agents).

# Elliptics
Reverbrain had already developed distributed storage system [Elliptics](http://reverbrain.com/elliptics),
it was originally build on a distributed hash table basis, but we introduced logical entities
named [buckets](http://doc.reverbrain.com/backrunner:backrunner#bucket) to wrap replication
into structure which can be easily operated on.
Our main goal for Elliptics was to create a really safe storage which would store replicas
across physically distributed datacenters, and with buckets it could be fine-tuned for different
types of data. Buckets allow horizontal scaling in the environments where there are constant
or increasing rate of input data and ever increasing storage size. When adding new buckets
for new servers there is no resharding or consistent hashing rebalancing,
although it is possible to scale single bucket (set of replicas) by adding servers into selected buckets.
Elliptics proved to handle scaling issues easily, in 2013 one of our installation had 53+ billions
of records, for comparison, Facebook had 450+ billions of photos these days.

# Greylock features
Having this background we decided to build search engine on Elliptics base.
This took off all scaling, replication, distribution and recovery tasks from our shoulders.
Basically, we concentrated on base search engine features, and that is what we have:
* Automatic scaling with Elliptics buckets support, no resharding, no data copy, no locked tables
and other related performance issues, one should just add new servers and they will be used immediately
after being added, which is particulary useful to take off write load or fix issues with not enough space.
* Automatic replication management via Elliptics buckets - one can put index replicas
into different datacenters across the globe, and reading operations will select the fastest replica
available for given client.
* HTTP JSON-based REST API, which is quite popular in modern indexing servers.
* Basic set of searching options like index intersection and simple query distance based relevance.
We do not try to implement any search option possible, but will add them on demand.
* Automatic index recovery - most of the time we do not have to wait until Elliptics recovery strikes in,
instead we can fix replicas when client writes new keys.
* Search load balancing - client always reads indexes from the fastest available replica
from the given bucket.

Greylock is a quite simple search engine, we created it to fill in scalability niche.
Because of this we do not have embedded natural language processing like lemmatization
(or simpler stemming), spelling error correction, synonim searching and so on.
Instead we build microservice architecture where NLP tasks are separated into its own service.
We will release it soon too.

# Examples
`conf/` directory among others contains `greylock_server` HTTP server config and insert/select json
files which are examples of the appropriate operations over HTTP. Files include all supported features.
To search and index via HTTP API one has to use `/search` and `/index` URLs accordingly.
Host and port where `greylock_server` HTTP server listens for incoming connection is specified
in its config in `endpoints` section. The most vital part - Elliptics connection and buckets
are in `application` section, which will be described in details in documentation.

# Issues
* Automatic scaling does has its price - Elliptics and Greylock are both built as an eventual consistency
systems, which means consistency is not guaranteed after some types of errors until recovery strikes in.
Greylock tries to fix indexes when performing update though.
* HTTP server and C++ API do not know about different clients - updating the same index from different
HTTP servers for example will lead to index corruption. Sharding of the clients (HTTP server is a client)
is fixed by introducing distibuted consensus on the client side, we use (Consul)[https://consul.io/]
on the client side. This is not a part of the search engine, but we will describe needed setup in
documentation.
* Set of features is rather limited, but these are trully what we use in daily basis, so we decided
not to overengineer the solution. New features will be added when required.
* Only string indexes are supported. There are no numeric indexes, but every key added into index
has a timestamp (uint64_t) and keys are sorted by timestamp + string ID. We do not yet support numerics
since these indexes will have different sorting order and thus paginated read will have to fetch whole
indexes, intersect them and return needed number of records. This will be slow for really large indexes.
But we do have plans to introduce numeric indexes.
