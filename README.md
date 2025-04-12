KANE
====

an opinionated key value database ORM for golang.
drop any object into a KV and search for any of its fields.

this is a reduced version of apogy, rebuilt on rawkv instead of relying on tikvs percolator,
which suffers under high contention.

if we are willing to sacrifice multi-object transactions,
we can make single object commits using zero-phase CAS and the timestamp oracle to create unique index keys.

contention still requires retrying the CAS because of a limitation in rawkw,
but indexing is longer involved in the contention, making the retry significanly more likely to pass.
