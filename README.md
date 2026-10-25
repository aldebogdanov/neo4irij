# neo4irij

Neo4j for [Irij](https://irij.online): Cypher as an effect, over the
[Neo4j Query API](https://neo4j.com/docs/query-api/current/) (HTTP, v2).

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
  speaks HTTP through std.http's `Http` effect. Swap either handler in
  tests: canned server replies, or canned records, no database needed.
- **No Java.** neo4irij is pure Irij over std.http, so nothing that uses it
  declares `JVM`: queries need `::: neo.Neo4j`, and a function that
  installs the real handler (`default-neo4j :: Neo4j ::: http.Http`)
  declares `::: http.Http`. `irij audit` finds no Java in it.
- **Exact values both ways.** Uses typed JSON (`application/vnd.neo4j.query`),
  so Integer and Float stay apart, nodes don't look like maps, and dates,
  durations and points round-trip.
- **Transactions** with commit / rollback-and-reraise, cluster affinity,
  bookmarks for causal consistency, read routing, impersonation, and
  retry of transient errors.

Needs a server with the Query API: Neo4j 5.19 or later (including the
202x.x calendar releases), or Aura. Tested against Neo4j 2026.10.

## Install

```toml
[seeds]
neo4irij = "0.1"
```

```irij
use neo4irij :as neo
use std.http :as http
```

## Connecting

```irij
conn := neo.connect {url= "http://localhost:7474" user= "neo4j" password= "secret"}
conn := neo.connect {url= "neo4j+s://abcd1234.databases.neo4j.io" user= "neo4j" password= "…"}  ;; Aura
conn := neo.connect {url= "https://db.example" token= "eyJ…"}                                   ;; bearer / SSO
conn := with env.default-env do (neo.connect-env ())   ;; NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
```

`connect` options, all optional: `url` (default `http://localhost:7474`),
`user` + `password`, `token` (wins over user/password), `database`
(default `neo4j`), `timeout-ms` (whole-request deadline; `0` waits).

`url` may be the Bolt URI your `.env` already has: `neo4j+s://h` and
`bolt+s://h` become `https://h`, `neo4j://h` and `bolt://h` become
`http://h:7474`, and an explicit Bolt port `7687` becomes 7473 / 7474.

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
| `info conn` | the server's discovery document (`neo4j_version`, `neo4j_edition`, …) |

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

The server rolls back a transaction left idle too long (a minute on a
default server); each statement extends it. In a cluster, the `neo4j-cluster-affinity` header
the server returns is sent back on every request of the transaction.

The effect's operations are there for finer control: `neo-begin conn`,
`neo-tx-run tx cypher params`, `neo-commit tx` (returns bookmarks),
`neo-rollback tx`.

## Errors and retries

A failed request raises (with `error`) a `Neo4jError {status code message}`.
`code` is Neo4j's status code, such as
`Neo.ClientError.Statement.SyntaxError` or
`Neo.ClientError.Schema.ConstraintValidationFailed`; failures that never
reached Neo4j are `neo4irij.HttpError` (non-2xx without a Neo4j error
body, e.g. a proxy's 502) or `neo4irij.BadResponse` (a reply that isn't
JSON). Connection failures surface as std.http raises them.

```irij
match (try (-> neo.execute conn "CREATE (:User {email: $e})" {e= email}))
  Ok _ => :created
  Err (neo.Neo4jError _ "Neo.ClientError.Schema.ConstraintValidationFailed" _) => :taken
  Err e => error e
```

`transient? e` is true for `Neo.TransientError.*` (deadlocks, leader
changes, …) and 5xx `neo4irij.HttpError`s. `retry attempts thunk` retries
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
the requests it builds — see `tests/test-wire.irj`.

## Limits

- The Query API returns a result in one response, so a query's whole
  result is held in memory; page large reads with `SKIP`/`LIMIT` or
  keyset pagination.
- HTTP only, no Bolt. One request per statement.
- Integers are 64-bit on the server; a larger Irij `Int` is rejected.

## Development

The tests need Irij 0.9.353 or later (they nest inline `with … do`).

```sh
irij test                    # unit tests; live tests skip without NEO4J_URI

docker run -d --rm -p 7474:7474 -e NEO4J_AUTH=neo4j/password123 neo4j:latest
NEO4J_URI=http://localhost:7474 NEO4J_USERNAME=neo4j NEO4J_PASSWORD=password123 irij test
```

`examples/movies.irj` is a small end-to-end tour against the same server.

## License

MIT
