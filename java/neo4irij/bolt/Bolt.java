package neo4irij.bolt;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.TypeSystem;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * neo4irij.bolt's cap provider: the Neo4j effect's operations over Bolt,
 * with Neo4j's official driver.
 *
 * <p>Values cross in the Query API's typed JSON — {@code {"$type": T,
 * "_value": v}} — both ways: parameters arrive as neo4irij.value encodes
 * them, and a statement returns a Query API response body
 * ({@code data.fields}, {@code data.values}, {@code counters},
 * {@code bookmarks}, {@code notifications}). So the Irij side decodes
 * Bolt with the HTTP decoder, and a value reads the same over either.
 *
 * <p>Every entry point returns {@code {"ok": value}} or
 * {@code {"error": {"code", "message"}}}: an exception thrown into Irij
 * would keep only its message, and callers match on Neo4j's code.
 *
 * <p>One driver (a connection pool) per Bolt URI and credentials, made
 * on first use; open explicit transactions by id. Both live here, so
 * Irij only ever passes plain values.
 */
public final class Bolt {

    private Bolt() {}

    private static final Map<String, Driver> DRIVERS = new ConcurrentHashMap<>();

    private record OpenTx(Session session, Transaction tx) {}

    private static final Map<String, OpenTx> TXS = new ConcurrentHashMap<>();

    private static final TypeSystem TYPES = TypeSystem.getDefault();

    // ── Entry points ───────────────────────────────────────────────────

    /** One statement in its own auto-commit transaction. {@code target}
     *  is {uri, scheme, user, password, token}; {@code session} is
     *  {database, mode, bookmarks, impersonate}. */
    public static Map<String, Object> run(Map<String, Object> target, Map<String, Object> session,
                                          String cypher, Map<String, Object> params, long timeoutMs) {
        return guard(() -> {
            try (Session s = driver(target).session(sessionConfig(session))) {
                Result r = s.run(new Query(cypher, fromTypedMap(params)), txConfig(timeoutMs));
                Map<String, Object> body = body(r);
                body.put("bookmarks", bookmarks(s));
                return body;
            }
        });
    }

    /** Open an explicit transaction; its id. */
    public static Map<String, Object> begin(Map<String, Object> target, Map<String, Object> session,
                                            long timeoutMs) {
        return guard(() -> {
            Session s = driver(target).session(sessionConfig(session));
            try {
                Transaction tx = s.beginTransaction(txConfig(timeoutMs));
                String id = UUID.randomUUID().toString();
                TXS.put(id, new OpenTx(s, tx));
                return id;
            } catch (RuntimeException e) {
                s.close();
                throw e;
            }
        });
    }

    /** One statement inside an open transaction. */
    public static Map<String, Object> txRun(String id, String cypher, Map<String, Object> params) {
        return guard(() -> {
            Map<String, Object> body = body(open(id).tx().run(new Query(cypher, fromTypedMap(params))));
            body.put("bookmarks", List.of());
            return body;
        });
    }

    /** Commit; the session's bookmarks. The transaction is done either way. */
    public static Map<String, Object> commit(String id) {
        return guard(() -> {
            OpenTx o = TXS.remove(id);
            if (o == null) throw notOpen(id);
            try {
                o.tx().commit();
                return bookmarks(o.session());
            } finally {
                o.session().close();
            }
        });
    }

    /** Roll back; a transaction already gone (failed, committed) is fine. */
    public static Map<String, Object> rollback(String id) {
        return guard(() -> {
            OpenTx o = TXS.remove(id);
            if (o != null) {
                try {
                    o.tx().rollback();
                } catch (RuntimeException ignored) {
                    // The server has dropped it already.
                } finally {
                    o.session().close();
                }
            }
            return null;
        });
    }

    /** neo4j_version and neo4j_edition, as the HTTP discovery document
     *  names them, and the Bolt URI. */
    public static Map<String, Object> info(Map<String, Object> target, Map<String, Object> session) {
        return guard(() -> {
            try (Session s = driver(target).session(sessionConfig(session))) {
                // One row per component; the kernel's comes first.
                Record rec = s.run("CALL dbms.components() YIELD versions, edition "
                        + "RETURN versions[0] AS version, edition").list().get(0);
                var m = new LinkedHashMap<String, Object>();
                m.put("neo4j_version", rec.get("version").asString());
                m.put("neo4j_edition", rec.get("edition").asString());
                m.put("bolt", target.get("uri"));
                return m;
            }
        });
    }

    /** Close the driver for {@code target}; a later statement opens a new one. */
    public static Map<String, Object> close(Map<String, Object> target) {
        return guard(() -> {
            Driver d = DRIVERS.remove(key(target));
            if (d != null) d.close();
            return null;
        });
    }

    /** A typed value through the driver's own types and back — what a
     *  parameter becomes on the server, without one. For tests. */
    public static Map<String, Object> roundTrip(Map<String, Object> typed) {
        return guard(() -> toTyped(Values.value(fromTyped(typed))));
    }

    // ── Errors ──────────────────────────────────────────────────────────

    private static Map<String, Object> guard(Supplier<Object> work) {
        var out = new HashMap<String, Object>();
        try {
            out.put("ok", work.get());
        } catch (Neo4jException e) {
            out.put("error", error(codeOf(e), e.getMessage()));
        } catch (RuntimeException e) {
            out.put("error", error("neo4irij.BoltError",
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
        return out;
    }

    /** No server to talk to is worth retrying (neo.transient?); a driver
     *  failure without a Neo4j code of its own is a BoltError. */
    static String codeOf(Neo4jException e) {
        if (e instanceof ServiceUnavailableException || e instanceof SessionExpiredException) {
            return "neo4irij.Unavailable";
        }
        String code = e.code();
        return code == null || code.isEmpty() || code.equals("N/A") ? "neo4irij.BoltError" : code;
    }

    private static Map<String, Object> error(String code, String message) {
        var m = new LinkedHashMap<String, Object>();
        m.put("code", code);
        m.put("message", message);
        return m;
    }

    private static IllegalStateException notOpen(String id) {
        return new IllegalStateException("transaction " + id + " is not open");
    }

    private static OpenTx open(String id) {
        OpenTx o = TXS.get(id);
        if (o == null) throw notOpen(id);
        return o;
    }

    // ── Drivers and sessions ────────────────────────────────────────────

    private static String key(Map<String, Object> target) {
        return target.get("uri") + "\u0000" + target.get("scheme") + "\u0000" + target.get("user")
                + "\u0000" + target.get("password") + "\u0000" + target.get("token");
    }

    private static Driver driver(Map<String, Object> target) {
        return DRIVERS.computeIfAbsent(key(target), k -> GraphDatabase.driver(
                (String) target.get("uri"), authToken(target),
                // The driver logs to stderr by default; a library shouldn't.
                Config.builder().withLogging(Logging.none()).build()));
    }

    private static AuthToken authToken(Map<String, Object> target) {
        return switch (String.valueOf(target.get("scheme"))) {
            case "basic" -> AuthTokens.basic((String) target.get("user"), (String) target.get("password"));
            case "bearer" -> AuthTokens.bearer((String) target.get("token"));
            default -> AuthTokens.none();
        };
    }

    @SuppressWarnings("unchecked")
    private static SessionConfig sessionConfig(Map<String, Object> session) {
        var b = SessionConfig.builder().withDatabase((String) session.get("database"));
        if ("READ".equals(session.get("mode"))) b.withDefaultAccessMode(AccessMode.READ);
        var marks = (List<Object>) session.getOrDefault("bookmarks", List.of());
        if (!marks.isEmpty()) {
            b.withBookmarks(marks.stream().map(m -> Bookmark.from(String.valueOf(m))).toList());
        }
        Object user = session.get("impersonate");
        if (user != null && !user.toString().isEmpty()) b.withImpersonatedUser(user.toString());
        return b.build();
    }

    private static TransactionConfig txConfig(long timeoutMs) {
        return timeoutMs > 0
                ? TransactionConfig.builder().withTimeout(java.time.Duration.ofMillis(timeoutMs)).build()
                : TransactionConfig.empty();
    }

    private static List<Object> bookmarks(Session s) {
        var out = new ArrayList<Object>();
        for (Bookmark b : s.lastBookmarks()) out.add(b.value());
        return out;
    }

    // ── Results as a Query API body ─────────────────────────────────────

    private static Map<String, Object> body(Result r) {
        List<String> fields = r.keys();
        var values = new ArrayList<Object>();
        while (r.hasNext()) {
            Record rec = r.next();
            var row = new ArrayList<Object>();
            for (Value v : rec.values()) row.add(toTyped(v));
            values.add(row);
        }
        ResultSummary summary = r.consume();
        var data = new LinkedHashMap<String, Object>();
        data.put("fields", new ArrayList<>(fields));
        data.put("values", values);
        var body = new LinkedHashMap<String, Object>();
        body.put("data", data);
        body.put("counters", counters(summary.counters()));
        body.put("notifications", notifications(summary));
        return body;
    }

    /** The Query API's counter names; neo4irij kebabs them. */
    private static Map<String, Object> counters(SummaryCounters c) {
        var m = new LinkedHashMap<String, Object>();
        m.put("containsUpdates", c.containsUpdates());
        m.put("nodesCreated", (long) c.nodesCreated());
        m.put("nodesDeleted", (long) c.nodesDeleted());
        m.put("propertiesSet", (long) c.propertiesSet());
        m.put("relationshipsCreated", (long) c.relationshipsCreated());
        m.put("relationshipsDeleted", (long) c.relationshipsDeleted());
        m.put("labelsAdded", (long) c.labelsAdded());
        m.put("labelsRemoved", (long) c.labelsRemoved());
        m.put("indexesAdded", (long) c.indexesAdded());
        m.put("indexesRemoved", (long) c.indexesRemoved());
        m.put("constraintsAdded", (long) c.constraintsAdded());
        m.put("constraintsRemoved", (long) c.constraintsRemoved());
        m.put("containsSystemUpdates", c.containsSystemUpdates());
        m.put("systemUpdates", (long) c.systemUpdates());
        return m;
    }

    private static List<Object> notifications(ResultSummary summary) {
        var out = new ArrayList<Object>();
        for (Notification n : summary.notifications()) {
            var m = new LinkedHashMap<String, Object>();
            m.put("code", n.code());
            m.put("title", n.title());
            m.put("description", n.description());
            m.put("severity", n.rawSeverityLevel().orElse(""));
            m.put("category", n.rawCategory().orElse(""));
            out.add(m);
        }
        return out;
    }

    // ── Typed JSON ⇄ driver values ──────────────────────────────────────

    private static Map<String, Object> typed(String kind, Object value) {
        var m = new HashMap<String, Object>();
        m.put("$type", kind);
        m.put("_value", value);
        return m;
    }

    /** A driver value as the Query API writes it — same kinds, same text. */
    static Map<String, Object> toTyped(Value v) {
        if (v.hasType(TYPES.NULL())) return typed("Null", null);
        if (v.hasType(TYPES.BOOLEAN())) return typed("Boolean", v.asBoolean());
        if (v.hasType(TYPES.INTEGER())) return typed("Integer", Long.toString(v.asLong()));
        if (v.hasType(TYPES.FLOAT())) return typed("Float", Double.toString(v.asDouble()));
        if (v.hasType(TYPES.STRING())) return typed("String", v.asString());
        if (v.hasType(TYPES.BYTES())) return typed("Base64", Base64.getEncoder().encodeToString(v.asByteArray()));
        if (v.hasType(TYPES.LIST())) {
            var xs = new ArrayList<Object>();
            for (Value x : v.values()) xs.add(toTyped(x));
            return typed("List", xs);
        }
        // Nodes and relationships are maps too, to the type system: before MAP.
        if (v.hasType(TYPES.NODE())) return typed("Node", node(v.asNode()));
        if (v.hasType(TYPES.RELATIONSHIP())) return typed("Relationship", rel(v.asRelationship()));
        if (v.hasType(TYPES.PATH())) return typed("Path", path(v.asPath()));
        if (v.hasType(TYPES.MAP())) return typed("Map", props(v));
        if (v.hasType(TYPES.DATE())) return typed("Date", DateTimeFormatter.ISO_LOCAL_DATE.format(v.asLocalDate()));
        if (v.hasType(TYPES.TIME())) return typed("Time", DateTimeFormatter.ISO_OFFSET_TIME.format(v.asOffsetTime()));
        if (v.hasType(TYPES.LOCAL_TIME())) return typed("LocalTime", DateTimeFormatter.ISO_LOCAL_TIME.format(v.asLocalTime()));
        if (v.hasType(TYPES.LOCAL_DATE_TIME())) {
            return typed("LocalDateTime", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v.asLocalDateTime()));
        }
        if (v.hasType(TYPES.DATE_TIME())) {
            ZonedDateTime z = v.asZonedDateTime();
            return z.getZone() instanceof ZoneOffset
                    ? typed("OffsetDateTime", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(z))
                    : typed("ZonedDateTime", DateTimeFormatter.ISO_ZONED_DATE_TIME.format(z));
        }
        if (v.hasType(TYPES.DURATION())) return typed("Duration", durationText(v.asIsoDuration()));
        if (v.hasType(TYPES.POINT())) return typed("Point", pointText(v.asPoint()));
        return typed(v.type().name(), String.valueOf(v.asObject()));
    }

    private static Map<String, Object> props(org.neo4j.driver.types.MapAccessor m) {
        var out = new LinkedHashMap<String, Object>();
        for (String k : m.keys()) out.put(k, toTyped(m.get(k)));
        return out;
    }

    private static Map<String, Object> node(Node n) {
        var m = new LinkedHashMap<String, Object>();
        m.put("_element_id", n.elementId());
        var labels = new ArrayList<Object>();
        for (String l : n.labels()) labels.add(l);
        m.put("_labels", labels);
        m.put("_properties", props(n));
        return m;
    }

    private static Map<String, Object> rel(Relationship r) {
        var m = new LinkedHashMap<String, Object>();
        m.put("_element_id", r.elementId());
        m.put("_start_node_element_id", r.startNodeElementId());
        m.put("_end_node_element_id", r.endNodeElementId());
        m.put("_type", r.type());
        m.put("_properties", props(r));
        return m;
    }

    /** As the Query API writes a path: node, rel, node, … */
    private static List<Object> path(Path p) {
        var out = new ArrayList<Object>();
        out.add(typed("Node", node(p.start())));
        for (Path.Segment s : p) {
            out.add(typed("Relationship", rel(s.relationship())));
            out.add(typed("Node", node(s.end())));
        }
        return out;
    }

    private static final BigInteger BILLION = BigInteger.valueOf(1_000_000_000L);
    private static final BigInteger NANOS_PER_HOUR = BigInteger.valueOf(3_600_000_000_000L);
    private static final BigInteger NANOS_PER_MINUTE = BigInteger.valueOf(60_000_000_000L);

    /** Neo4j's own rendering: years and months from the months, days as
     *  they are, hours, minutes and seconds from the time — each signed,
     *  zero parts left out, "PT0S" for nothing at all. */
    static String durationText(IsoDuration d) {
        long months = d.months();
        BigInteger total = BigInteger.valueOf(d.seconds()).multiply(BILLION).add(BigInteger.valueOf(d.nanoseconds()));
        BigInteger hours = total.divide(NANOS_PER_HOUR);
        BigInteger minutes = total.remainder(NANOS_PER_HOUR).divide(NANOS_PER_MINUTE);
        BigInteger secNanos = total.remainder(NANOS_PER_MINUTE);
        String date = part(months / 12, "Y") + part(months % 12, "M") + part(d.days(), "D");
        String time = part(hours.longValueExact(), "H") + part(minutes.longValueExact(), "M") + seconds(secNanos);
        if (date.isEmpty() && time.isEmpty()) return "PT0S";
        return "P" + date + (time.isEmpty() ? "" : "T" + time);
    }

    private static String part(long n, String unit) {
        return n == 0 ? "" : n + unit;
    }

    private static String seconds(BigInteger ns) {
        if (ns.signum() == 0) return "";
        BigInteger whole = ns.divide(BILLION);
        long fraction = ns.remainder(BILLION).abs().longValueExact();
        String sign = ns.signum() < 0 && whole.signum() == 0 ? "-" : "";
        String digits = "";
        if (fraction != 0) {
            digits = "." + String.format("%09d", fraction).replaceAll("0+$", "");
        }
        return sign + whole + digits + "S";
    }

    private static String pointText(Point p) {
        String coords = Double.isNaN(p.z())
                ? "POINT (" + p.x() + " " + p.y() + ")"
                : "POINT Z (" + p.x() + " " + p.y() + " " + p.z() + ")";
        return "SRID=" + p.srid() + ";" + coords;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fromTypedMap(Map<String, Object> params) {
        var out = new HashMap<String, Object>();
        for (var e : params.entrySet()) out.put(e.getKey(), fromTyped((Map<String, Object>) e.getValue()));
        return out;
    }

    /** A typed value as the driver takes it. */
    @SuppressWarnings("unchecked")
    static Object fromTyped(Map<String, Object> t) {
        String kind = String.valueOf(t.get("$type"));
        Object v = t.get("_value");
        return switch (kind) {
            case "Null" -> null;
            case "Boolean", "String" -> v;
            case "Integer" -> {
                try {
                    yield Long.parseLong((String) v);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(v + " is past a Cypher Integer's 64 bits");
                }
            }
            case "Float" -> Double.parseDouble((String) v);
            case "List" -> {
                var xs = new ArrayList<Object>();
                for (Object x : (List<Object>) v) xs.add(fromTyped((Map<String, Object>) x));
                yield xs;
            }
            case "Map" -> fromTypedMap((Map<String, Object>) v);
            case "Date" -> LocalDate.parse((String) v);
            case "Time" -> OffsetTime.parse((String) v);
            case "LocalTime" -> LocalTime.parse((String) v);
            case "LocalDateTime" -> LocalDateTime.parse((String) v);
            case "OffsetDateTime" -> OffsetDateTime.parse((String) v);
            case "ZonedDateTime" -> ZonedDateTime.parse((String) v);
            case "Duration" -> durationValue((String) v);
            case "Point" -> pointValue((String) v);
            case "Base64" -> Base64.getDecoder().decode((String) v);
            default -> throw new IllegalArgumentException("a Tagged " + kind + " can't be sent over Bolt");
        };
    }

    /** ISO-8601 as Neo4j writes it ("P1Y2M3DT4H5M6.5S", "P-1DT-1H") or the
     *  driver does ("P14M3DT14706.5S"): months and days through Period,
     *  the time part through Duration. */
    static Value durationValue(String s) {
        int t = s.indexOf('T');
        String datePart = t < 0 ? s : s.substring(0, t);
        String timePart = t < 0 ? "" : s.substring(t + 1);
        Period period = datePart.equals("P") ? Period.ZERO : Period.parse(datePart);
        java.time.Duration span = timePart.isEmpty() ? java.time.Duration.ZERO : java.time.Duration.parse("PT" + timePart);
        return Values.isoDuration(period.toTotalMonths(), period.getDays(), span.getSeconds(), span.getNano());
    }

    /** "SRID=4326;POINT (13.4 52.5)" or "SRID=9157;POINT Z (1.0 2.0 3.0)". */
    static Value pointValue(String s) {
        int srid = Integer.parseInt(s.substring(5, s.indexOf(';')));
        String[] xs = s.substring(s.indexOf('(') + 1, s.indexOf(')')).trim().split("\\s+");
        return xs.length == 3
                ? Values.point(srid, Double.parseDouble(xs[0]), Double.parseDouble(xs[1]), Double.parseDouble(xs[2]))
                : Values.point(srid, Double.parseDouble(xs[0]), Double.parseDouble(xs[1]));
    }
}
