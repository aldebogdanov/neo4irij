# neo4irij

Neo4j for [Irij](https://irij.online): Cypher as an effect, over the
[Neo4j Query API](https://neo4j.com/docs/query-api/current/) (HTTP, v2)
or over Bolt with Neo4j's official Java driver.

```irij
use std.http :as http
use neo4irij :as neo

conn := neo.connect {url= "http://localhost:7474" user= "neo4j" password= "secret"}

with http.default-http
  with neo.default-neo4j
    neo.execute conn "CREATE (:Person {name: $name, born: $born})" {name= "Ada" born= (neo.date "1815-12-10")}
    people := neo.query conn "MATCH (p:Person) RETURN p.name AS name, p.born AS born" {}
    ;; #[{name= "Ada" born= Tagged {kind= "Date" text= "1815-12-10"}}]
```

- **Effect, not a client object.** `Neo4j` is an effect; `default-neo4j`
  speaks HTTP through std.http's `Http` effect, `neo4irij.bolt`'s `neo4j`
  speaks Bolt. Your code doesn't change with the transport, and in tests
  you swap in canned server replies or canned records, no database needed.
- **No `JVM` in your rows.** The HTTP side is pure Irij over std.http: a
  function that installs `default-neo4j :: Neo4j ::: http.Http` declares
  `::: http.Http`. Bolt's driver work is Java the seed ships in `java/`,
  bound with a `cap`, so `bolt.neo4j :: Neo4j ::: Neo4j` and a function
  installing it declares `::: neo.Neo4j` — the resource it touches, not
  "anything". `irij audit` finds one cap and no hand-written Java.
- **Exact values both ways.** Uses typed JSON (`application/vnd.neo4j.query`),
  so Integer and Float stay apart, nodes don't look like maps, and dates,
  durations and points round-trip.
- **Transactions** with commit / rollback-and-reraise, cluster affinity,
  bookmarks for causal consistency, read routing, impersonation, and
  retry of transient errors.

Over HTTP it needs a server with the Query API: Neo4j 5.19 or later
(including the 202x.x calendar releases), or Aura. Bolt works with any
server the driver (6.3) supports. Tested against Neo4j 2026.10.

## Install

```toml
[seeds]
neo4irij = "0.1"

[trust]
jars = ["neo4irij"]        # the seed brings the Bolt driver, used or not
java = ["neo4irij"]        # …and its Java provider, compiled from java/
caps = ["neo4irij.bolt"]   # only if you use Bolt: it binds the provider
```

neo4irij lists the Bolt driver (`org.neo4j.driver:neo4j-java-driver-all`,
one shaded jar of about 6 MB plus `reactive-streams`) in its `[jvm]`, and
ships `java/neo4irij/bolt/Bolt.java`, which Irij compiles against it (once;
the jar is cached in `~/.irij/java`). Both come with the seed whichever
transport you use. A project with a `[trust]` section — which
`irij build` and `irij publish` require — names the seed under `jars` and
`java`, or Irij refuses it before downloading or compiling anything; one
without `[trust]` gets a warning from `irij run`. A project that uses Bolt
also lets `neo4irij.bolt` bind the provider with `caps`. Compiling needs
a JDK, as Irij itself does.

```irij
use neo4irij :as neo
use std.http :as http          ;; over HTTP
use neo4irij.bolt :as bolt     ;; over Bolt
```

## Connecting

```irij
conn := neo.connect {url= "http://localhost:7474" user= "neo4j" password= "secret"}
conn := neo.connect {url= "neo4j+s://abcd1234.databases.neo4j.io" user= "neo4j" password= "…"}  ;; Aura
conn := neo.connect {url= "https://db.example" token= "eyJ…"}                                   ;; bearer / SSO
conn := with env.default-env do (neo.connect-env ())   ;; NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
```

`connect` options, all optional: `url` (default `http://localhost:7474`),
`bolt-url`, `user` + `password`, `token` (wins over user/password),
`database` (default `neo4j`), `timeout-ms` (a deadline per request, or per
transaction over Bolt; `0` waits).

One `Conn` serves both transports: it keeps the server's HTTP endpoint
(`conn.url`) and its Bolt URI (`conn.bolt`), both worked out from `url`:

| `url` | HTTP | Bolt |
|---|---|---|
| `http://h:7474` | `http://h:7474` | `neo4j://h` (port 7687) |
| `https://h` | `https://h` | `neo4j+s://h` |
| `neo4j+s://h`, `bolt+s://h` | `https://h` | as given |
| `neo4j://h`, `bolt://h` | `http://h:7474` | as given |

An explicit Bolt port `7687` maps to HTTP's 7473 / 7474. When Bolt
listens somewhere else, say so with `bolt-url` (or `NEO4J_BOLT_URI` for
`connect-env`): `neo.connect {url= "http://h:17474" bolt-url= "neo4j://h:17687"}`.

A `Conn` is plain data. Adjust it with:

| fn | |
|---|---|
| `use-database conn "movies"` | another database |
| `read-only conn` | `accessMode: READ` — routes to readers; writes fail |
| `with-bookmarks conn bms` | wait until the server has seen these bookmarks |
| `impersonate conn "jo"` | run as another user (needs IMPERSONATE) |
| `with-timeout conn 5000` | request deadline in ms |

A `Conn` holds the credentials, so don't log it.

## Running Cypher

Every helper takes a **target**: a `Conn` (the statement runs in its own
auto-commit transaction) or a `Tx` (inside an explicit transaction).
Parameters are a Map; refer to them as `$name`.

| fn | returns |
|---|---|
| `query target cypher params` | `Vec` of records, each a Map keyed by the `RETURN` names |
| `single target cypher params` | the first record, or `()` |
| `scalar target cypher params` | the first column of the first record, or `()` |
| `execute target cypher params` | the update counters: `{nodes-created= 1 properties-set= 2 …}` |
| `run target cypher params` | the whole `QueryResult` |
| `info conn` | the server's `neo4j_version` and `neo4j_edition` (over HTTP its whole discovery document) |
| `close conn` | over Bolt, closes the Conn's driver and connections; nothing over HTTP |

```irij
n     := neo.scalar conn "MATCH (p:Person) RETURN count(p)" {}
ada   := neo.single conn "MATCH (p:Person {name: $name}) RETURN p" {name= "Ada"}
stats := neo.execute conn "MATCH (p:Person) SET p.seen = true" {}
r     := neo.run conn "MATCH (p:Person) RETURN p.name AS name" {}
;; r.fields r.records r.counters r.bookmarks r.notifications
```

`QueryResult` fields: `fields` (the `RETURN` names), `records`,
`counters` (kebab-case keys), `bookmarks`, `notifications` (the server's
warnings: unknown labels, cartesian products, deprecations, …, as Maps).

## Values

| Cypher | Irij |
|---|---|
| `null` | `()` |
| Boolean, String | `Bool`, `Str` |
| Integer | `Int` |
| Float | `Float` (NaN and ±Infinity included) |
| List | `Vec` — sent from a `Vec`, `Tuple` or `Set` |
| Map | `Map` |
| Node | `Node {id labels props}` |
| Relationship | `Rel {id type start end props}` — `start`/`end` are node ids |
| Path | `Path {nodes rels}` |
| Date, Time, LocalTime, LocalDateTime, DateTime, Duration, Point, bytes, … | `Tagged {kind text}` |

A Rational is sent as a Float and a keyword as its name (`:gold` →
`"gold"`). Nodes, relationships and paths can't be parameters; pass an id
or properties. `id` is Neo4j's elementId: stable within a transaction,
not across them, so key long-lived references on a property you own.

`Tagged` values read back can be sent again unchanged. Build new ones
with `date`, `time`, `local-time`, `local-datetime`, `datetime` (offset,
or zoned when the text has `[Zone/Id]`), `duration` (ISO-8601, `"P1DT2H"`),
`point` (`"SRID=4326;POINT (13.4 52.5)"`), `wgs84 lon lat`, and `bytes`
(base64).

```irij
match neo.single conn "MATCH p = (:Person)-[:KNOWS*]->(:Person) RETURN p LIMIT 1" {}
  {p= path} => path.nodes |> @ (n -> get "name" n.props)
  _ => #[]
```

## Transactions

`transaction conn work` opens a transaction, calls `work tx`, commits
when it returns, and rolls back and re-raises when it fails. Its value is
`work`'s.

```irij
fn transfer :: neo.Tx _ ::: neo.Neo4j
  => tx
  neo.execute tx "MATCH (a:Account {id: $id}) SET a.balance = a.balance - $n" {id= 1 n= 10}
  neo.execute tx "MATCH (a:Account {id: $id}) SET a.balance = a.balance + $n" {id= 2 n= 10}

neo.transaction conn transfer
```

Over HTTP the server rolls back a transaction left idle too long (a
minute on a default server); each statement extends it. In a cluster, the
`neo4j-cluster-affinity` header the server returns is sent back on every
request of the transaction. Over Bolt a transaction holds a session, and
its connection, until it commits or rolls back.

The effect's operations are there for finer control: `neo-begin conn`,
`neo-tx-run tx cypher params`, `neo-commit tx` (returns bookmarks),
`neo-rollback tx`, `neo-close conn`.

## Bolt

```irij
use neo4irij :as neo
use neo4irij.bolt :as bolt

conn := neo.connect {url= "neo4j+s://abcd1234.databases.neo4j.io" user= "neo4j" password= "…"}

fn people :: neo.Conn Vec ::: neo.Neo4j
  => conn
  with bolt.neo4j
    neo.query conn "MATCH (p:Person) RETURN p.name AS name" {}
```

`bolt.neo4j` handles the same `Neo4j` effect as `default-neo4j`, so
everything above — `query` to `retry`, `Conn` adjustments, a fake
handler in tests — works over Bolt unchanged, and values come back the
same: the same Ints, Nodes and Rels, and the same `Tagged` text for every
temporal, spatial and duration value (the live tests read one battery of
values over both and compare them).

- **How it's built.** `java/neo4irij/bolt/Bolt.java` is a cap provider:
  static methods over plain values that open sessions, run statements and
  hold transactions. Values cross in the Query API's typed JSON both
  ways, so Bolt results are decoded by the same code as HTTP's, and a
  failure comes back as data with Neo4j's code.
- **One driver per server and user.** The first statement for a `Conn`'s
  Bolt URI and credentials creates a driver, which owns a connection pool;
  later statements, from any fiber, reuse it. `neo.close conn` closes it.
  A program that exits doesn't have to.
- **Routing.** A `neo4j://` / `neo4j+s://` URI routes through the cluster:
  `read-only` sends reads to followers and read replicas. `bolt://` talks
  to the one server.
- **Bookmarks.** Each auto-commit statement's `QueryResult.bookmarks` and
  `commit`'s return value are the session's bookmarks; `with-bookmarks`
  passes them on, as over HTTP.
- **Errors** carry the same Neo4j codes as over HTTP, with `status` 0.
  A server that can't be reached is `neo4irij.Unavailable`, which
  `transient?` counts; any other driver failure without a Neo4j code is
  `neo4irij.BoltError`.

## Errors and retries

A failed request raises (with `error`) a `Neo4jError {status code message}`.
`code` is Neo4j's status code, such as
`Neo.ClientError.Statement.SyntaxError` or
`Neo.ClientError.Schema.ConstraintValidationFailed`; failures that never
reached Neo4j are `neo4irij.HttpError` (non-2xx without a Neo4j error
body, e.g. a proxy's 502) or `neo4irij.BadResponse` (a reply that isn't
JSON). Over HTTP, connection failures surface as std.http raises them;
over Bolt they are `neo4irij.Unavailable` (see Bolt).

```irij
match (try (-> neo.execute conn "CREATE (:User {email: $e})" {e= email}))
  Ok _ => :created
  Err (neo.Neo4jError _ "Neo.ClientError.Schema.ConstraintValidationFailed" _) => :taken
  Err e => error e
```

`transient? e` is true for `Neo.TransientError.*` (deadlocks, leader
changes, …), 5xx `neo4irij.HttpError`s and `neo4irij.Unavailable`. `retry attempts thunk` retries
those with backoff (100ms, 200ms, 400ms, …) and re-raises anything else:

```irij
neo.retry 5 (-> neo.transaction conn transfer)
```

Retry the whole transaction, not a statement inside one: after an error
the server has already rolled it back.

## Testing your code

Code that performs `Neo4j` runs under any handler of it:

```irij
handler fake-neo4j :: neo.Neo4j
  neo-run conn cypher params =>
    resume (neo.QueryResult #["name"] #[{name= "Ada"}] {} #[] #[])

with fake-neo4j
  assert-eq #[{name= "Ada"}] (neo.query conn "MATCH (p:Person) RETURN p.name AS name" {})
```

Or keep `default-neo4j` and fake the HTTP underneath, which also checks
the requests it builds — see `tests/test-wire.irj`. Either way the code
under test needs no driver and no `JVM`.

## Limits

- A query's whole result is held in memory, over either transport (the
  Query API returns it in one response; over Bolt the records are
  collected before `query` returns). Page large reads with
  `SKIP`/`LIMIT` or keyset pagination.
- Integers are 64-bit on the server; a larger Irij `Int` is rejected.
- Over Bolt, a `Tagged` value of a kind the driver has no type for (such
  as `Vector`) can be read but not sent back.

## Development

neo4irij needs an Irij with Java sources (`java/`, spec §3.3): the first
release after 0.11.376.

```sh
irij test                    # unit tests; live tests skip without NEO4J_URI

docker run -d --rm -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password123 neo4j:latest
NEO4J_URI=http://localhost:7474 NEO4J_USERNAME=neo4j NEO4J_PASSWORD=password123 irij test
```

The live tests run every scenario over HTTP and again over Bolt
(`NEO4J_BOLT_URI` when Bolt isn't at port 7687), then compare what both
transports read. `examples/movies.irj` is a small end-to-end tour against
the same server; `irij movies.irj bolt` takes it over Bolt.

## License

MIT
