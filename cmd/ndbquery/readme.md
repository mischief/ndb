ndbquery: simple ndb reader
========

if /usr/local/plan9/ndb/root-servers contains a file with an ndb entry such as

    dom=A.ROOT-SERVERS.NET ip=198.41.0.4

a call to ndbquery might go like so:

    $ ndbquery -f /usr/local/plan9/ndb/root-servers dom A.ROOT-SERVERS.NET ip
    198.41.0.4

