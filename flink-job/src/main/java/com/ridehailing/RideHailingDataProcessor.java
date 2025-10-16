package com.ridehailing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/** Realtime KPI: requests theo op='c'; status chuẩn hoá; bucket theo event-time; flags chống double-count. */
public class RideHailingDataProcessor {

    // Kafka
    static final String KAFKA_BOOTSTRAP = "kafka:19092";
    static final String TOPIC_BOOKING = "ridehailing.public.booking";
    static final String TOPIC_DRIVER_LOCATION = "ridehailing.public.driver_location";

    // Reporting DB
    static final String REPORTING_JDBC_URL  = "jdbc:postgresql://postgres-reporting:5432/reporting_db";
    static final String REPORTING_JDBC_USER = "user";
    static final String REPORTING_JDBC_PASS = "password";

    // Elasticsearch
    static final String ES_HOSTNAME = "elasticsearch";
    static final int    ES_PORT     = 9200;

    // ==== Models ====
    static class BookingEvent {
        String bookingId, status, serviceType, serviceTier, areaCode, op;
        Double platformFee, discountAmount;
        Instant createdAt, startTripAt, completedAt, updatedAt;
    }
    static class MetricDelta {
        String serviceType = "", serviceTier = "", areaCode = "";
        Instant bucketStart;
        long requests, accepted, completed, canceled;
        double revenue, discount, gmv;
    }
    static class DriverLocDoc {
        String driverId, availability;
        double lat, lon; Instant updatedAt;
    }
    /** Element cho bảng KPI (1 element = 1 row JDBC) */
    static class KpiRow {
        String kpiName, serviceType = "", serviceTier = "", areaCode = "";
        double kpiValue; Instant eventTime = Instant.now();
    }
    /** Cờ chống double-count theo vòng đời job */
    public static class BookingFlags implements java.io.Serializable {
        public boolean requested, accepted, completed, canceled;
        public String lastStatus;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---- Kafka sources ----
        KafkaSource<ObjectNode> bookingSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC_BOOKING)
                .setGroupId("flink-booking-realtime")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        KafkaSource<ObjectNode> driverLocSource = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC_DRIVER_LOCATION)
                .setGroupId("flink-driverloc-realtime")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        // ---- BOOKING: CDC -> event ----
        DataStream<BookingEvent> bookingEvents = env
                .fromSource(bookingSource, WatermarkStrategy.noWatermarks(), "booking-source")
                .map((MapFunction<ObjectNode, BookingEvent>) RideHailingDataProcessor::mapBooking)
                .filter(e -> e != null && e.bookingId != null && !e.bookingId.isEmpty())
                // bỏ snapshot 'r' để không đổ số lịch sử vào realtime
                .filter(e -> !"r".equalsIgnoreCase(e.op));

        // ---- Keyed by bookingId -> phát delta theo flags + bucket event-time ----
        DataStream<MetricDelta> deltas = bookingEvents
                .keyBy((KeySelector<BookingEvent, String>) e -> e.bookingId)
                .process(new KeyedProcessFunction<String, BookingEvent, MetricDelta>() {

                    private transient ValueState<BookingFlags> state;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("flags", BookingFlags.class));
                    }

                    @Override
                    public void processElement(BookingEvent e, Context ctx, Collector<MetricDelta> out) throws Exception {
                        BookingFlags f = state.value();
                        if (f == null) f = new BookingFlags();

                        String st = nvl(e.serviceType), tier = nvl(e.serviceTier), area = nvl(e.areaCode);

                        // (1) REQUESTED: chỉ khi op='c'
                        if ("c".equalsIgnoreCase(e.op) && !f.requested) {
                            Instant et = e.createdAt != null ? e.createdAt :
                                         (e.updatedAt != null ? e.updatedAt : Instant.now());
                            out.collect(delta(et, st, tier, area, 1,0,0,0, 0,0,0));
                            f.requested = true;
                        }

                        // Chuẩn hoá status
                        String s = e.status == null ? "" : e.status.toUpperCase();

                        // (2) ACCEPTED: chứa "ACCEPT"
                        if (s.contains("ACCEPT") && !f.accepted) {
                            Instant et = e.updatedAt != null ? e.updatedAt : Instant.now();
                            out.collect(delta(et, st, tier, area, 0,1,0,0, 0,0,0));
                            f.accepted = true;
                        }

                        // (3) COMPLETED: chứa "COMPLET"
                        if (s.contains("COMPLET") && !f.completed) {
                            Instant et = e.completedAt != null ? e.completedAt :
                                         (e.updatedAt != null ? e.updatedAt : Instant.now());
                            double rev = safe(e.platformFee), disc = safe(e.discountAmount);
                            out.collect(delta(et, st, tier, area, 0,0,1,0, rev+disc, rev, disc));
                            f.completed = true;
                        }

                        // (4) CANCELED: chứa "CANCEL"
                        if (s.contains("CANCEL") && !f.canceled) {
                            Instant et = e.updatedAt != null ? e.updatedAt : Instant.now();
                            out.collect(delta(et, st, tier, area, 0,0,0,1, 0,0,0));
                            f.canceled = true;
                        }

                        f.lastStatus = e.status;
                        state.update(f);
                    }

                    private MetricDelta delta(Instant eventTime, String st, String tier, String area,
                                              long req, long acc, long comp, long canc,
                                              double gmv, double rev, double disc) {
                        MetricDelta d = new MetricDelta();
                        d.bucketStart = truncateToMinute(eventTime);
                        d.serviceType = st; d.serviceTier = tier; d.areaCode = area;
                        d.requests = req; d.accepted = acc; d.completed = comp; d.canceled = canc;
                        d.gmv = gmv; d.revenue = rev; d.discount = disc;
                        return d;
                    }
                });

        // ---- Sink 1: fact_trip_minute (UPsert theo bucket) ----
        deltas.addSink(JdbcSink.sink(
                "INSERT INTO mart.fact_trip_minute (" +
                        "bucket_start, service_type, service_tier, area_code, " +
                        "requests_total, accepted_total, completed_total, canceled_total, " +
                        "gmv_total, revenue_total, discount_amount_total, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) " +
                "ON CONFLICT (bucket_start, service_type, service_tier, area_code) DO UPDATE SET " +
                        "requests_total        = mart.fact_trip_minute.requests_total        + EXCLUDED.requests_total, " +
                        "accepted_total        = mart.fact_trip_minute.accepted_total        + EXCLUDED.accepted_total, " +
                        "completed_total       = mart.fact_trip_minute.completed_total       + EXCLUDED.completed_total, " +
                        "canceled_total        = mart.fact_trip_minute.canceled_total        + EXCLUDED.canceled_total, " +
                        "gmv_total             = mart.fact_trip_minute.gmv_total             + EXCLUDED.gmv_total, " +
                        "revenue_total         = mart.fact_trip_minute.revenue_total         + EXCLUDED.revenue_total, " +
                        "discount_amount_total = mart.fact_trip_minute.discount_amount_total + EXCLUDED.discount_amount_total, " +
                        "last_updated = now()",
                (ps, d) -> {
                    ps.setTimestamp(1, java.sql.Timestamp.from(d.bucketStart));
                    ps.setString(2, d.serviceType);
                    ps.setString(3, d.serviceTier);
                    ps.setString(4, d.areaCode);
                    ps.setLong(5, d.requests);
                    ps.setLong(6, d.accepted);
                    ps.setLong(7, d.completed);
                    ps.setLong(8, d.canceled);
                    ps.setDouble(9, d.gmv);
                    ps.setDouble(10, d.revenue);
                    ps.setDouble(11, d.discount);
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(300).withBatchSize(200).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        // ---- TÁCH KPI ROWS (mỗi KPI là 1 element) ----
        DataStream<KpiRow> kpiRows = deltas.flatMap(new FlatMapFunction<MetricDelta, KpiRow>() {
            @Override public void flatMap(MetricDelta d, Collector<KpiRow> out) {
                if (d.requests  != 0) out.collect(k("total_requests", d.requests));
                if (d.accepted  != 0) out.collect(k("accepted_total", d.accepted));
                if (d.completed != 0) out.collect(k("completed_total", d.completed));
                if (d.canceled  != 0) out.collect(k("canceled_total",  d.canceled));
                if (d.revenue   != 0) out.collect(k("revenue_total",   d.revenue));
                if (d.discount  != 0) out.collect(k("discount_total",  d.discount));
                if (d.gmv       != 0) out.collect(k("gmv_total",       d.gmv));
            }
            private KpiRow k(String name, double v) {
                KpiRow r = new KpiRow();
                r.kpiName = name; r.kpiValue = v; r.eventTime = Instant.now();
                return r;
            }
        });

        // ---- Sink 2: latest_kpi_by_scope (mỗi element 1 row) ----
        kpiRows.addSink(JdbcSink.sink(
                "INSERT INTO mart.latest_kpi_by_scope " +
                  "(kpi_name, service_type, service_tier, area_code, kpi_value, event_time, last_updated) " +
                "VALUES (?, '', '', '', ?, ?, now()) " +
                "ON CONFLICT (kpi_name, service_type, service_tier, area_code) DO UPDATE SET " +
                  "kpi_value = mart.latest_kpi_by_scope.kpi_value + EXCLUDED.kpi_value, " +
                  "event_time = EXCLUDED.event_time, last_updated = now()",
                (ps, r) -> {
                    ps.setString(1, r.kpiName);
                    ps.setDouble(2, r.kpiValue);
                    ps.setObject(3, r.eventTime);
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(300).withBatchSize(500).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        // ---- DRIVER LOCATION → Elasticsearch ----
        DataStream<DriverLocDoc> driverLocs = env
                .fromSource(driverLocSource, WatermarkStrategy.noWatermarks(), "driverloc-source")
                .map((MapFunction<ObjectNode, DriverLocDoc>) RideHailingDataProcessor::mapDriverLocation)
                .filter(d -> d != null && d.driverId != null && !d.driverId.isEmpty());

        driverLocs.sinkTo(
            new Elasticsearch7SinkBuilder<DriverLocDoc>()
                .setBulkFlushMaxActions(500)
                .setHosts(new org.apache.http.HttpHost(ES_HOSTNAME, ES_PORT, "http"))
                .setEmitter((ElasticsearchEmitter<DriverLocDoc>) (e, ctx, indexer) -> {
                    IndexRequest req = Requests.indexRequest()
                        .index("driver_locations")
                        .id(e.driverId)
                        .source("driverId", e.driverId,
                                "availability", e.availability,
                                "@timestamp", e.updatedAt.toString(),
                                "location", new java.util.HashMap<String, Object>() {{
                                    put("lat", e.lat);
                                    put("lon", e.lon);
                                }});
                    indexer.add(req);
                }).build()
        );

        env.execute("RideHailing Data Processor — Realtime + EventTime + Flags");
    }

    // ==== Helpers ==== //
    static String nvl(String s) { return s == null ? "" : s; }
    static double safe(Double d) { return d == null ? 0.0 : d; }
    static Instant truncateToMinute(Instant t) {
        ZonedDateTime z = t.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        return z.toInstant();
    }

    static BookingEvent mapBooking(ObjectNode node) {
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) return null;
        BookingEvent e = new BookingEvent();
        e.op            = node.has("op") ? node.get("op").asText() : "u";
        e.bookingId     = getTextCI(after, "id");
        e.status        = getTextCI(after, "status");
        e.serviceType   = getTextCI(after, "serviceTypeCode");
        e.serviceTier   = getTextCI(after, "serviceTierCode");
        e.areaCode      = getTextCI(after, "wardCode");
        e.platformFee   = getDoubleCI(after, "platformFee");
        e.discountAmount= getDoubleCI(after, "discountAmount");
        e.createdAt     = parseInstant(getTextCI(after, "createdAt"));
        e.startTripAt   = parseInstant(getTextCI(after, "startTripAt"));
        e.completedAt   = parseInstant(getTextCI(after, "completedAt"));
        e.updatedAt     = parseInstant(getTextCI(after, "updatedAt"));
        return e;
    }

    static DriverLocDoc mapDriverLocation(ObjectNode node) {
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) return null;
        DriverLocDoc d = new DriverLocDoc();
        d.driverId = getTextCI(after, "driverId");
        d.availability = getTextCI(after, "availability");

        JsonNode latNode = getFieldCI(after, "latitude");
        JsonNode lonNode = getFieldCI(after, "longitude");
        if (latNode != null && !latNode.isNull() && lonNode != null && !lonNode.isNull()) {
            d.lat = latNode.asDouble(); d.lon = lonNode.asDouble();
        } else {
            JsonNode coords = after.get("coordinates");
            if (coords == null || coords.isNull()) return null;
            try {
                if (coords.isObject() && coords.has("wkb") && !coords.get("wkb").isNull()) {
                    String b64 = coords.get("wkb").asText();
                    byte[] bytes = java.util.Base64.getDecoder().decode(b64);
                    org.locationtech.jts.geom.Point p =
                        (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(bytes);
                    d.lon = p.getX(); d.lat = p.getY();
                } else if (coords.isTextual()) {
                    String s = coords.asText();
                    try {
                        byte[] bytes = java.util.Base64.getDecoder().decode(s);
                        org.locationtech.jts.geom.Point p =
                            (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(bytes);
                        d.lon = p.getX(); d.lat = p.getY();
                    } catch (IllegalArgumentException ex) {
                        byte[] hex = hexStringToByteArray(s);
                        org.locationtech.jts.geom.Point p =
                            (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(hex);
                        d.lon = p.getX(); d.lat = p.getY();
                    }
                } else return null;
            } catch (Throwable t) { return null; }
        }
        d.updatedAt = Instant.now();
        return d;
    }

    // Case-insensitive getters
    static String getTextCI(JsonNode obj, String field) {
        JsonNode n = getFieldCI(obj, field);
        return (n == null || n.isNull()) ? null : n.asText();
    }
    static double getDoubleCI(JsonNode obj, String field) {
        JsonNode n = getFieldCI(obj, field);
        if (n == null || n.isNull()) return 0d;
        try { return n.asDouble(); } catch (Exception e) { return 0d; }
    }
    static JsonNode getFieldCI(JsonNode obj, String field) {
        if (obj.has(field)) return obj.get(field);
        String lower = field.toLowerCase();
        if (obj.has(lower)) return obj.get(lower);
        String upper = field.toUpperCase();
        if (obj.has(upper)) return obj.get(upper);
        return null;
    }
    static Instant parseInstant(String s) {
        try { return (s == null || s.isEmpty()) ? null : Instant.parse(s); }
        catch (Exception ignore) { return null; }
    }
    static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        ByteBuffer buf = ByteBuffer.allocate(len / 2);
        for (int i = 0; i < len; i += 2) {
            buf.put((byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16)));
        }
        return buf.array();
    }
}
