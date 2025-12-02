package com.ridehailing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.client.CredentialsProvider;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Realtime KPI: requests theo op='c'; status chuẩn hoá; bucket theo event-time; flags chống double-count. */
public class RideHailingDataProcessor {

    // Kafka
    static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP",
            "kafka-1:19092");
    static final String TOPIC_BOOKING = "ridehailing.public.booking";
    static final String TOPIC_DRIVER_LOCATION = "ridehailing.public.driver_location";
    static final String TOPIC_PASSENGER = "ridehailing.public.passenger";
    static final String TOPIC_DRIVER = "ridehailing.public.driver";
    static final Pattern TOPIC_BOOKING_PATTERN = Pattern.compile(Pattern.quote(TOPIC_BOOKING));
    static final Pattern TOPIC_DRIVER_LOC_PATTERN = Pattern.compile(Pattern.quote(TOPIC_DRIVER_LOCATION));
    static final Pattern TOPIC_PASSENGER_PATTERN = Pattern.compile(Pattern.quote(TOPIC_PASSENGER));
    static final Pattern TOPIC_DRIVER_PATTERN = Pattern.compile(Pattern.quote(TOPIC_DRIVER));

    // Reporting DB
    static final String REPORTING_JDBC_URL  = "jdbc:postgresql://postgres-reporting:5432/reporting_db";
    static final String REPORTING_JDBC_USER = "user";
    static final String REPORTING_JDBC_PASS = "password";

    static final double VAT_RATE = 0.10;
    static final double NET_REVENUE_FACTOR = 1.0 / (1.0 + VAT_RATE);

    // Elasticsearch
    static final String ES_HOSTNAME = "elasticsearch";
    static final int    ES_PORT     = 9200;
    static final String ES_USERNAME = System.getenv().getOrDefault("ES_USERNAME", "elastic");
    static final String ES_PASSWORD = System.getenv().getOrDefault("ELASTIC_PASSWORD", "changeme123");

    static final long PASSENGER_CHURN_WINDOW_MS = Duration.ofDays(3).toMillis();
    static final long DRIVER_CHURN_WINDOW_MS    = Duration.ofDays(3).toMillis();
    static final Duration MAX_OUT_OF_ORDERNESS  = Duration.ofMinutes(5);

    static final ZoneId REPORTING_ZONE_ID = ZoneId.of(
            System.getenv().getOrDefault("REPORTING_TZ", "Asia/Ho_Chi_Minh"));

    static final DateTimeFormatter ES_TIMESERIES_INDEX_FORMAT =
            DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);

    static final StateTtlConfig STATE_TTL_30_DAYS = StateTtlConfig
            .newBuilder(Time.days(30))
            .cleanupFullSnapshot()
            .build();

    static final OutputTag<PassengerActivityEvent> PASSENGER_ACTIVITY_TAG =
            new OutputTag<PassengerActivityEvent>("passenger-activity") {};
    static final OutputTag<BookingRouteSegment> BOOKING_ROUTE_TAG =
            new OutputTag<BookingRouteSegment>("booking-route") {};

    static final MapStateDescriptor<String, DriverProfile> DRIVER_PROFILE_BROADCAST_STATE =
            new MapStateDescriptor<>("driver-profile", String.class, DriverProfile.class);

    static class DriverProfile implements java.io.Serializable {
        String serviceType = "";
        String serviceTier = "";
        String areaCode = "";
    }

    // ==== Models ====
    static class BookingEvent {
        String bookingId, status, serviceType, serviceTier, areaCode, op;
        String passengerId;
        Double platformFee, discountAmount;
        Double pickupLat, pickupLon, dropoffLat, dropoffLon;
        Instant createdAt, acceptedAt, startTripAt, completedAt, updatedAt;
    }
    static class PassengerEvent {
        String passengerId, op;
        Instant createdAt, updatedAt;
    }
    static class DriverEvent {
        String driverId, op;
        String serviceType, serviceTier, areaCode;
        Instant createdAt, updatedAt;
    }
    static class MetricRecord {
        String metricName;
        String serviceType = "", serviceTier = "", areaCode = "";
        String cohortKey = "ALL";
        String entityType = "AGGREGATE";
        String entityId   = "ALL";
        Instant bucketStart;
        String bucketGranularity = "MINUTE";
        Instant eventTime;
        Instant processedAt;
        double metricValue;
        double numeratorValue;
        double denominatorValue;
        double sampleSize;
        int processingLagMs;
        Instant firstEventAt;
        Instant lastEventAt;
        String metricUnit = "COUNT";
    }
    static class DriverLocDoc {
        String driverId, availability;
        String serviceType = "", serviceTier = "", areaCode = "";
        double lat, lon; Instant updatedAt;
    }
    static class PassengerActivityEvent {
        String passengerId;
        String type;
        Instant eventTime;
    }

    static class BookingRouteSegment {
        String bookingId;
        String segmentType = "PATH";
        int sequenceNo;
        double latitude;
        double longitude;
        Instant eventTime;
    }
    /** Trạng thái booking để chống double-count và giữ timestamp trung gian */
    public static class BookingState implements java.io.Serializable {
        public boolean requested, accepted, completed, canceled;
        public String lastStatus;
        public Instant createdAt;
        public Instant acceptedAt;
        public Instant startedAt;
        public Instant completedAt;
        public double lastPlatformFee;
        public double lastDiscountAmount;
        public boolean orderDurationEmitted;
        public boolean acceptDurationEmitted;
        public boolean boardDurationEmitted;
        public boolean leadDurationEmitted;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureStateBackend(env);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///flink-checkpoints");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---- Kafka sources ----
        OffsetsInitializer bookingOffsets = offsetsInitializer("FLINK_BOOKING_OFFSET_MODE", "LATEST");
        OffsetsInitializer driverLocOffsets = offsetsInitializer("FLINK_DRIVERLOC_OFFSET_MODE", "LATEST");
        OffsetsInitializer passengerOffsets = offsetsInitializer("FLINK_PASSENGER_OFFSET_MODE", "LATEST");
        OffsetsInitializer driverOffsets = offsetsInitializer("FLINK_DRIVER_OFFSET_MODE", "LATEST");

KafkaSource<ObjectNode> bookingSource = KafkaSource.<ObjectNode>builder()
.setBootstrapServers(KAFKA_BOOTSTRAP)
.setTopics(TOPIC_BOOKING)
.setGroupId("flink-booking-realtime")
.setStartingOffsets(bookingOffsets)
.setPartitionDiscoveryInterval(Duration.ofSeconds(15))
.setProperty("allow.auto.create.topics", "true")
.setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
.build();

KafkaSource<ObjectNode> driverLocSource = KafkaSource.<ObjectNode>builder()
.setBootstrapServers(KAFKA_BOOTSTRAP)
.setTopics(TOPIC_DRIVER_LOCATION)
.setGroupId("flink-driverloc-realtime")
.setStartingOffsets(driverLocOffsets)
.setPartitionDiscoveryInterval(Duration.ofSeconds(15))
.setProperty("allow.auto.create.topics", "true")
.setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
.build();

KafkaSource<ObjectNode> passengerSource = KafkaSource.<ObjectNode>builder()
.setBootstrapServers(KAFKA_BOOTSTRAP)
.setTopics(TOPIC_PASSENGER)
.setGroupId("flink-passenger-realtime")
.setStartingOffsets(passengerOffsets)
.setPartitionDiscoveryInterval(Duration.ofSeconds(15))
.setProperty("allow.auto.create.topics", "true")
.setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
.build();

KafkaSource<ObjectNode> driverSource = KafkaSource.<ObjectNode>builder()
.setBootstrapServers(KAFKA_BOOTSTRAP)
.setTopics(TOPIC_DRIVER)
.setGroupId("flink-driver-registrations")
.setStartingOffsets(driverOffsets)
.setPartitionDiscoveryInterval(Duration.ofSeconds(15))
                .setProperty("allow.auto.create.topics", "true")
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        WatermarkStrategy<ObjectNode> watermarkStrategy = watermarkByUpdatedAt();

        // ---- BOOKING: CDC -> event ----
        DataStream<BookingEvent> bookingEvents = env
                .fromSource(bookingSource, watermarkStrategy, "booking-source")
                .map((MapFunction<ObjectNode, BookingEvent>) RideHailingDataProcessor::mapBooking)
                .filter(e -> e != null && e.bookingId != null && !e.bookingId.isEmpty())
                // bỏ snapshot 'r' để không đổ số lịch sử vào realtime
                .filter(e -> !"r".equalsIgnoreCase(e.op));

        // ---- Keyed by bookingId -> phát các metric record tương ứng KPI ----
        SingleOutputStreamOperator<MetricRecord> bookingMetrics = bookingEvents
                .keyBy((KeySelector<BookingEvent, String>) e -> e.bookingId)
                .process(new BookingMetricProcessor());

        DataStream<PassengerActivityEvent> passengerActivityEvents =
                bookingMetrics.getSideOutput(PASSENGER_ACTIVITY_TAG);

        DataStream<BookingRouteSegment> bookingRouteSegments =
                bookingMetrics.getSideOutput(BOOKING_ROUTE_TAG);

        // ---- DRIVER LOCATION → Elasticsearch ----
        DataStream<DriverEvent> driverEvents = env
                .fromSource(driverSource, watermarkStrategy, "driver-source")
                .map((MapFunction<ObjectNode, DriverEvent>) RideHailingDataProcessor::mapDriver)
                .filter(d -> d != null && d.driverId != null && !d.driverId.isEmpty())
                .filter(d -> !"r".equalsIgnoreCase(d.op));

        BroadcastStream<DriverEvent> driverProfileBroadcast = driverEvents
                .broadcast(DRIVER_PROFILE_BROADCAST_STATE);

        SingleOutputStreamOperator<DriverLocDoc> enrichedDriverLocs = env
                .fromSource(driverLocSource, watermarkStrategy, "driverloc-source")
                .map((MapFunction<ObjectNode, DriverLocDoc>) RideHailingDataProcessor::mapDriverLocation)
                .filter(d -> d != null && d.driverId != null && !d.driverId.isEmpty())
                .connect(driverProfileBroadcast)
                .process(new DriverLocationEnrichmentFunction());

        DataStream<MetricRecord> driverMetrics = enrichedDriverLocs
                .keyBy((KeySelector<DriverLocDoc, String>) d -> d.driverId)
                .process(new DriverMetricProcessor());

        DataStream<MetricRecord> passengerMetrics = env
                .fromSource(passengerSource, watermarkStrategy, "passenger-source")
                .map((MapFunction<ObjectNode, PassengerEvent>) RideHailingDataProcessor::mapPassenger)
                .filter(p -> p != null && p.passengerId != null && !p.passengerId.isEmpty())
                .filter(p -> !"r".equalsIgnoreCase(p.op))
                .keyBy((KeySelector<PassengerEvent, String>) p -> p.passengerId)
                .process(new PassengerMetricProcessor());

        DataStream<MetricRecord> passengerActivityMetrics = passengerActivityEvents
                .filter(evt -> evt != null && evt.passengerId != null && !evt.passengerId.isEmpty())
                .keyBy((KeySelector<PassengerActivityEvent, String>) evt -> evt.passengerId)
                .process(new PassengerActivityProcessor());

        DataStream<MetricRecord> driverRegistrationMetrics = driverEvents
                .keyBy((KeySelector<DriverEvent, String>) d -> d.driverId)
                .process(new DriverRegistrationProcessor());

        DataStream<MetricRecord> allMetrics = bookingMetrics
                .union(driverMetrics, passengerMetrics, passengerActivityMetrics, driverRegistrationMetrics);

        SingleOutputStreamOperator<MetricRecord> aggregatedMetrics = allMetrics
                .keyBy((KeySelector<MetricRecord, String>) RideHailingDataProcessor::metricKey)
                .process(new MetricBucketAggregator());

        aggregatedMetrics.addSink(JdbcSink.sink(
                "INSERT INTO mart.fact_metric_bucket (" +
                        "bucket_start, bucket_granularity, metric_name, service_type, service_tier, area_code, " +
                        "cohort_key, entity_type, entity_id, metric_value, numerator_value, denominator_value, sample_size, " +
                        "first_event_at, last_event_at, last_ingested_at, processing_lag_ms, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) " +
                "ON CONFLICT (metric_name, bucket_start, bucket_granularity, service_type, service_tier, area_code, cohort_key, entity_type, entity_id) " +
                "DO UPDATE SET " +
                    "metric_value = EXCLUDED.metric_value, " +
                    "numerator_value = EXCLUDED.numerator_value, " +
                    "denominator_value = EXCLUDED.denominator_value, " +
                    "sample_size = EXCLUDED.sample_size, " +
                    "first_event_at = LEAST(COALESCE(mart.fact_metric_bucket.first_event_at, EXCLUDED.first_event_at), EXCLUDED.first_event_at), " +
                    "last_event_at = GREATEST(COALESCE(mart.fact_metric_bucket.last_event_at, EXCLUDED.last_event_at), EXCLUDED.last_event_at), " +
                    "last_ingested_at = GREATEST(mart.fact_metric_bucket.last_ingested_at, EXCLUDED.last_ingested_at), " +
                    "processing_lag_ms = EXCLUDED.processing_lag_ms, " +
                    "last_updated = now()",
                (ps, r) -> {
                    ps.setTimestamp(1, java.sql.Timestamp.from(r.bucketStart));
                    ps.setString(2, r.bucketGranularity);
                    ps.setString(3, r.metricName);
                    ps.setString(4, r.serviceType);
                    ps.setString(5, r.serviceTier);
                    ps.setString(6, r.areaCode);
                    ps.setString(7, r.cohortKey);
                    ps.setString(8, r.entityType);
                    ps.setString(9, r.entityId);
                    ps.setDouble(10, r.metricValue);
                    ps.setDouble(11, r.numeratorValue);
                    ps.setDouble(12, r.denominatorValue);
                    ps.setDouble(13, r.sampleSize);
                    ps.setTimestamp(14, java.sql.Timestamp.from(r.firstEventAt));
                    ps.setTimestamp(15, java.sql.Timestamp.from(r.lastEventAt));
                    ps.setTimestamp(16, java.sql.Timestamp.from(r.processedAt));
                    ps.setInt(17, r.processingLagMs);
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(300).withBatchSize(200).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        aggregatedMetrics.addSink(JdbcSink.sink(
                "INSERT INTO mart.latest_metric_snapshot (" +
                        "metric_name, service_type, service_tier, area_code, cohort_key, entity_type, entity_id, " +
                        "metric_value, numerator_value, denominator_value, sample_size, metric_unit, data_granularity, event_time, last_ingested_at, last_updated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) " +
                "ON CONFLICT (metric_name, service_type, service_tier, area_code, cohort_key, entity_type, entity_id) DO UPDATE SET " +
                    "metric_value = EXCLUDED.metric_value, " +
                    "numerator_value = EXCLUDED.numerator_value, " +
                    "denominator_value = EXCLUDED.denominator_value, " +
                    "sample_size = EXCLUDED.sample_size, " +
                    "metric_unit = EXCLUDED.metric_unit, " +
                    "data_granularity = EXCLUDED.data_granularity, " +
                    "event_time = GREATEST(mart.latest_metric_snapshot.event_time, EXCLUDED.event_time), " +
                    "last_ingested_at = EXCLUDED.last_ingested_at, " +
                    "last_updated = now()",
                (ps, r) -> {
                    ps.setString(1, r.metricName);
                    ps.setString(2, r.serviceType);
                    ps.setString(3, r.serviceTier);
                    ps.setString(4, r.areaCode);
                    ps.setString(5, r.cohortKey);
                    ps.setString(6, r.entityType);
                    ps.setString(7, r.entityId);
                    ps.setDouble(8, r.metricValue);
                    ps.setDouble(9, r.numeratorValue);
                    ps.setDouble(10, r.denominatorValue);
                    ps.setDouble(11, r.sampleSize);
                    ps.setString(12, r.metricUnit);
                    ps.setString(13, r.bucketGranularity);
                    ps.setTimestamp(14, java.sql.Timestamp.from(r.eventTime));
                    ps.setTimestamp(15, java.sql.Timestamp.from(r.processedAt));
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(500).withBatchSize(500).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        enrichedDriverLocs.addSink(JdbcSink.sink(
                "INSERT INTO mart.driver_location_timeseries (" +
                        "driver_id, service_type, service_tier, area_code, latitude, longitude, driver_status, event_time, received_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, now()) " +
                "ON CONFLICT (driver_id, event_time) DO UPDATE SET " +
                        "service_type = EXCLUDED.service_type, " +
                        "service_tier = EXCLUDED.service_tier, " +
                        "area_code = EXCLUDED.area_code, " +
                        "latitude = EXCLUDED.latitude, " +
                        "longitude = EXCLUDED.longitude, " +
                        "driver_status = EXCLUDED.driver_status, " +
                        "received_at = now()",
                (ps, d) -> {
                    ps.setString(1, d.driverId);
                    ps.setString(2, nvl(d.serviceType));
                    ps.setString(3, nvl(d.serviceTier));
                    ps.setString(4, nvl(d.areaCode));
                    ps.setDouble(5, d.lat);
                    ps.setDouble(6, d.lon);
                    ps.setString(7, nvl(d.availability));
                    ps.setTimestamp(8, java.sql.Timestamp.from(d.updatedAt));
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(500).withBatchSize(500).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        Elasticsearch7SinkBuilder<DriverLocDoc> latestSink = new Elasticsearch7SinkBuilder<DriverLocDoc>()
                .setBulkFlushMaxActions(500)
                .setHosts(new org.apache.http.HttpHost(ES_HOSTNAME, ES_PORT, "http"))
                .setEmitter((ElasticsearchEmitter<DriverLocDoc>) (e, ctx, indexer) -> {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("driverId", e.driverId);
                    doc.put("availability", nvl(e.availability));
                    doc.put("serviceType", nvl(e.serviceType));
                    doc.put("serviceTier", nvl(e.serviceTier));
                    doc.put("areaCode", nvl(e.areaCode));
                    doc.put("@timestamp", e.updatedAt.toString());
                    Map<String, Object> location = new HashMap<>();
                    location.put("lat", e.lat);
                    location.put("lon", e.lon);
                    doc.put("location", location);
                    IndexRequest req = Requests.indexRequest()
                        .index("driver_locations")
                        .id(e.driverId)
                        .source(doc);
                    indexer.add(req);
                });
        configureElasticAuth(latestSink);
        enrichedDriverLocs.sinkTo(latestSink.build());

        Elasticsearch7SinkBuilder<DriverLocDoc> timeseriesSink = new Elasticsearch7SinkBuilder<DriverLocDoc>()
                .setBulkFlushMaxActions(1000)
                .setHosts(new org.apache.http.HttpHost(ES_HOSTNAME, ES_PORT, "http"))
                .setEmitter((ElasticsearchEmitter<DriverLocDoc>) (e, ctx, indexer) -> {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("driverId", e.driverId);
                    doc.put("availability", nvl(e.availability));
                    doc.put("serviceType", nvl(e.serviceType));
                    doc.put("serviceTier", nvl(e.serviceTier));
                    doc.put("areaCode", nvl(e.areaCode));
                    doc.put("@timestamp", e.updatedAt.toString());
                    Map<String, Object> location = new HashMap<>();
                    location.put("lat", e.lat);
                    location.put("lon", e.lon);
                    doc.put("location", location);
                    String indexName = "driver_locations_timeseries-" + ES_TIMESERIES_INDEX_FORMAT.format(e.updatedAt);
                    IndexRequest req = Requests.indexRequest()
                        .index(indexName)
                        .source(doc);
                    indexer.add(req);
                });
        configureElasticAuth(timeseriesSink);
        enrichedDriverLocs.sinkTo(timeseriesSink.build());

        bookingRouteSegments.addSink(JdbcSink.sink(
                "INSERT INTO mart.booking_route_segments (" +
                        "booking_id, segment_type, sequence_no, latitude, longitude, event_time, received_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, now()) " +
                "ON CONFLICT (booking_id, segment_type, sequence_no) DO UPDATE SET " +
                        "latitude = EXCLUDED.latitude, " +
                        "longitude = EXCLUDED.longitude, " +
                        "event_time = EXCLUDED.event_time, " +
                        "received_at = now()",
                (ps, seg) -> {
                    ps.setString(1, seg.bookingId);
                    ps.setString(2, nvl(seg.segmentType));
                    ps.setInt(3, seg.sequenceNo);
                    ps.setDouble(4, seg.latitude);
                    ps.setDouble(5, seg.longitude);
                    ps.setTimestamp(6, seg.eventTime == null
                            ? new java.sql.Timestamp(System.currentTimeMillis())
                            : java.sql.Timestamp.from(seg.eventTime));
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(500).withBatchSize(500).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(REPORTING_JDBC_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(REPORTING_JDBC_USER)
                        .withPassword(REPORTING_JDBC_PASS)
                        .build()
        ));

        env.execute("RideHailing Data Processor — Realtime + EventTime + Flags");
    }

    private static void configureStateBackend(StreamExecutionEnvironment env) {
        try {
            org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend backend =
                    new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend();
            env.setStateBackend(backend);
        } catch (NoClassDefFoundError | Exception ex) {
            System.err.printf("[RideHailingDataProcessor] RocksDB backend unavailable (%s); using default state backend.%n",
                    ex.getMessage());
        }
    }

    // ==== Metric processors ==== //
    static class BookingMetricProcessor extends KeyedProcessFunction<String, BookingEvent, MetricRecord> {
        private transient ValueState<BookingState> state;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<BookingState> descriptor =
                    new ValueStateDescriptor<>("booking-metrics", BookingState.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(BookingEvent e, Context ctx, Collector<MetricRecord> out) throws Exception {
            BookingState bs = state.value();
            if (bs == null) bs = new BookingState();

            List<MetricRecord> metrics = new ArrayList<>();
            String st = nvl(e.serviceType), tier = nvl(e.serviceTier), area = nvl(e.areaCode);
            Instant now = Instant.now();

            if (bs.createdAt == null && e.createdAt != null) {
                bs.createdAt = e.createdAt;
            }

            if ("c".equalsIgnoreCase(e.op) && !bs.requested) {
                Instant eventTime = coalesce(e.createdAt, e.updatedAt, now);
                bs.createdAt = eventTime;
                metrics.add(metric("request_order", 1d, eventTime, "COUNT", st, tier, area));
                metrics.add(ratioMetric("acceptance_rate", 0d, 1d, eventTime, "PERCENT", st, tier, area));
                metrics.add(ratioMetric("completed_rate", 0d, 1d, eventTime, "PERCENT", st, tier, area));
                metrics.add(ratioMetric("cancel_rate", 0d, 1d, eventTime, "PERCENT", st, tier, area));
                emitPassengerActivity(ctx, e.passengerId, "REQUESTED", eventTime);
                emitRoute(ctx, e, eventTime);
                bs.requested = true;
            }

            String status = e.status == null ? "" : e.status.toUpperCase(Locale.ROOT);

            if (status.contains("ACCEPT") && !bs.accepted) {
                Instant acceptTs = coalesce(e.acceptedAt, e.updatedAt, now);
                bs.acceptedAt = acceptTs;
                metrics.add(metric("accept_order", 1d, acceptTs, "COUNT", st, tier, area));
                metrics.add(ratioMetric("acceptance_rate", 1d, 0d, acceptTs, "PERCENT", st, tier, area));
                emitPassengerActivity(ctx, e.passengerId, "ACCEPTED", acceptTs);
                Double acceptSeconds = secondsBetween(bs.createdAt, bs.acceptedAt);
                if (!bs.acceptDurationEmitted && acceptSeconds != null && acceptSeconds >= 0) {
                    metrics.add(metricWithSample("accept_time_total", acceptSeconds, acceptTs, "SECOND", st, tier, area, acceptSeconds, 1d, 1d));
                    metrics.add(durationMetric("accept_time", acceptSeconds, acceptTs, st, tier, area, 1d));
                    metrics.add(metricWithSample("order_time_total", acceptSeconds, acceptTs, "SECOND", st, tier, area, acceptSeconds, 1d, 1d));
                    metrics.add(durationMetric("order_time", acceptSeconds, acceptTs, st, tier, area, 1d));
                    bs.acceptDurationEmitted = true;
                    bs.orderDurationEmitted = true;
                }
                bs.accepted = true;
            }

            if (status.contains("START")) {
                bs.startedAt = coalesce(e.startTripAt, e.updatedAt, bs.startedAt);
            }

            if (status.contains("COMPLET") && !bs.completed) {
                Instant completeTs = coalesce(e.completedAt, e.updatedAt, now);
                bs.completedAt = completeTs;
                metrics.add(metric("completed_order", 1d, completeTs, "COUNT", st, tier, area));
                metrics.add(ratioMetric("completed_rate", 1d, 0d, completeTs, "PERCENT", st, tier, area));
                metrics.add(ratioMetric("productivity", 1d, 0d, completeTs, "TRIP_PER_DRIVER", st, tier, area));
                emitPassengerActivity(ctx, e.passengerId, "COMPLETED", completeTs);

                double platformFee = safe(e.platformFee);
                double discount = safe(e.discountAmount);
                bs.lastPlatformFee = platformFee;
                bs.lastDiscountAmount = discount;

                double gsv = platformFee + discount;
                double revenueVat = platformFee;
                double revenueNet = revenueVat * NET_REVENUE_FACTOR;
                double netDriverIncome = Math.max(0d, gsv - revenueVat);

                metrics.add(metric("gsv", gsv, completeTs, "CURRENCY", st, tier, area));
                metrics.add(metric("revenue_vat", revenueVat, completeTs, "CURRENCY", st, tier, area));
                metrics.add(metric("revenue_net", revenueNet, completeTs, "CURRENCY", st, tier, area));
                metrics.add(metric("net_driver_income", netDriverIncome, completeTs, "CURRENCY", st, tier, area));
                if (discount > 0d) {
                    metrics.add(metric("discount_order", discount, completeTs, "CURRENCY", st, tier, area));
                }

                Double orderSeconds = secondsBetween(bs.createdAt, bs.acceptedAt);
                if (!bs.orderDurationEmitted && orderSeconds != null && orderSeconds >= 0) {
                    metrics.add(metricWithSample("order_time_total", orderSeconds, completeTs, "SECOND", st, tier, area, orderSeconds, 1d, 1d));
                    metrics.add(durationMetric("order_time", orderSeconds, completeTs, st, tier, area, 1d));
                    metrics.add(durationMetric("accept_time", orderSeconds, completeTs, st, tier, area, 1d));
                    bs.orderDurationEmitted = true;
                    bs.acceptDurationEmitted = true;
                }
                Double boardSeconds = secondsBetween(bs.startedAt, bs.completedAt);
                if (!bs.boardDurationEmitted && boardSeconds != null && boardSeconds >= 0) {
                    metrics.add(metricWithSample("board_time_total", boardSeconds, completeTs, "SECOND", st, tier, area, boardSeconds, 1d, 1d));
                    metrics.add(durationMetric("board_time", boardSeconds, completeTs, st, tier, area, 1d));
                    bs.boardDurationEmitted = true;
                }
                Double leadSeconds = secondsBetween(bs.createdAt, bs.completedAt);
                if (!bs.leadDurationEmitted && leadSeconds != null && leadSeconds >= 0) {
                    metrics.add(metricWithSample("lead_time_total", leadSeconds, completeTs, "SECOND", st, tier, area, leadSeconds, 1d, 1d));
                    metrics.add(durationMetric("lead_time", leadSeconds, completeTs, st, tier, area, 1d));
                    bs.leadDurationEmitted = true;
                }

                bs.completed = true;
            }

            if (status.contains("CANCEL") && !bs.canceled) {
                Instant cancelTs = coalesce(e.updatedAt, e.createdAt, now);
                metrics.add(metric("cancel_order", 1d, cancelTs, "COUNT", st, tier, area));
                metrics.add(ratioMetric("cancel_rate", 1d, 0d, cancelTs, "PERCENT", st, tier, area));
                emitPassengerActivity(ctx, e.passengerId, "CANCELED", cancelTs);
                bs.canceled = true;
            }

            bs.lastStatus = e.status;

            for (MetricRecord m : metrics) {
                emitMetric(m, out);
            }

            if (bs.completed || bs.canceled) {
                state.clear();
            } else {
                state.update(bs);
            }
        }
    }

    static class PassengerMetricProcessor extends KeyedProcessFunction<String, PassengerEvent, MetricRecord> {
        private transient ValueState<PassengerRegistrationState> state;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<PassengerRegistrationState> descriptor =
                    new ValueStateDescriptor<>("passenger-registration", PassengerRegistrationState.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(PassengerEvent event, Context ctx, Collector<MetricRecord> out) throws Exception {
            PassengerRegistrationState st = state.value();
            if (st == null) st = new PassengerRegistrationState();

            if ("c".equalsIgnoreCase(event.op) && !st.created) {
                Instant eventTime = coalesce(event.createdAt, event.updatedAt, Instant.now());
                emitMetric(metric("new_passenger", 1d, eventTime, "COUNT", "", "", ""), out);
                st.created = true;
            }

            state.update(st);
        }
    }

    static class PassengerActivityProcessor extends KeyedProcessFunction<String, PassengerActivityEvent, MetricRecord> {
        private transient ValueState<PassengerActivityState> state;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<PassengerActivityState> descriptor =
                    new ValueStateDescriptor<>("passenger-activity", PassengerActivityState.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(PassengerActivityEvent event, Context ctx, Collector<MetricRecord> out) throws Exception {
            if (event == null || event.eventTime == null) {
                event = event == null ? new PassengerActivityEvent() : event;
                event.eventTime = Instant.now();
            }

            PassengerActivityState st = state.value();
            if (st == null) st = new PassengerActivityState();
            boolean wasChurned = st.churned;

            if ("COMPLETED".equalsIgnoreCase(event.type)) {
                Instant dayBucket = truncateToGranularity(event.eventTime, "DAY");
                if (st.lastActiveDay == null || !st.lastActiveDay.equals(dayBucket)) {
                    emitMetric(metric("active_passenger", 1d, event.eventTime, "COUNT", "", "", ""), out);

                    if (st.lastActiveDay != null) {
                        long diff = ChronoUnit.DAYS.between(st.lastActiveDay, dayBucket);
                        if (diff == 1) {
                            emitMetric(metric("retention_passenger", 1d, event.eventTime, "COUNT", "", "", ""), out);
                        } else if (diff >= 3 && wasChurned) {
                            emitMetric(metric("return_passenger", 1d, event.eventTime, "COUNT", "", "", ""), out);
                        }
                    }

                    st.lastActiveDay = dayBucket;
                }

                st.churned = false;
                Instant eventTime = event.eventTime;
                if (st.churnTimerTs != null) {
                    ctx.timerService().deleteEventTimeTimer(st.churnTimerTs);
                }
                long timerTs = eventTime.plusMillis(PASSENGER_CHURN_WINDOW_MS).toEpochMilli();
                st.churnTimerTs = timerTs;
                ctx.timerService().registerEventTimeTimer(timerTs);
            }

            state.update(st);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricRecord> out) throws Exception {
            if (ctx.timeDomain() != TimeDomain.EVENT_TIME) {
                return;
            }
            PassengerActivityState st = state.value();
            if (st == null || st.churned) {
                return;
            }
            Instant eventTime = Instant.ofEpochMilli(timestamp);
            emitMetric(metric("churn_passenger", 1d, eventTime, "COUNT", "", "", ""), out);
            st.churned = true;
            st.churnTimerTs = null;
            state.update(st);
        }
    }

    static class DriverMetricProcessor extends KeyedProcessFunction<String, DriverLocDoc, MetricRecord> {
        private transient ValueState<DriverStatusState> state;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<DriverStatusState> descriptor =
                    new ValueStateDescriptor<>("driver-status", DriverStatusState.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(DriverLocDoc doc, Context ctx, Collector<MetricRecord> out) throws Exception {
            DriverStatusState st = state.value();
            if (st == null) st = new DriverStatusState();

            Instant eventTime = doc.updatedAt != null ? doc.updatedAt : Instant.now();
            Instant bucket = truncateToMinute(eventTime);
            String availability = nvl(doc.availability).toUpperCase(Locale.ROOT);

            String svcType = resolve(doc.serviceType, st.serviceType);
            String svcTier = resolve(doc.serviceTier, st.serviceTier);
            String area = resolve(doc.areaCode, st.areaCode);

            doc.serviceType = svcType;
            doc.serviceTier = svcTier;
            doc.areaCode = area;

            boolean bucketChanged = st.lastBucketStart == null || !st.lastBucketStart.equals(bucket);
            boolean statusChanged = st.lastAvailability == null || !st.lastAvailability.equals(availability);
            boolean wasChurned = st.churned;

            if (bucketChanged || statusChanged) {
                if (isOnline(availability)) {
                    emitMetric(metric("online_driver", 1d, eventTime, "COUNT", svcType, svcTier, area), out);
                }
                if ("BUSY".equals(availability)) {
                    emitMetric(metric("busy_driver", 1d, eventTime, "COUNT", svcType, svcTier, area), out);
                }
            }

            if ("BUSY".equals(availability)) {
                Instant dayBucket = truncateToGranularity(eventTime, "DAY");
                if (st.lastBusyDay == null || !st.lastBusyDay.equals(dayBucket)) {
                    emitMetric(metric("active_driver", 1d, eventTime, "COUNT", svcType, svcTier, area), out);
                    emitMetric(ratioMetric("productivity", 0d, 1d, eventTime, "TRIP_PER_DRIVER", svcType, svcTier, area), out);
                    if (st.lastBusyDay != null) {
                        long diff = ChronoUnit.DAYS.between(st.lastBusyDay, dayBucket);
                        if (diff == 1) {
                            emitMetric(metric("retention_driver", 1d, eventTime, "COUNT", svcType, svcTier, area), out);
                        } else if (diff >= 3 && wasChurned) {
                            emitMetric(metric("return_driver", 1d, eventTime, "COUNT", svcType, svcTier, area), out);
                        }
                    }
                    st.lastBusyDay = dayBucket;
                }

                st.churned = false;
                if (st.churnTimerTs != null) {
                    ctx.timerService().deleteEventTimeTimer(st.churnTimerTs);
                }
                long timerTs = eventTime.plusMillis(DRIVER_CHURN_WINDOW_MS).toEpochMilli();
                st.churnTimerTs = timerTs;
                ctx.timerService().registerEventTimeTimer(timerTs);
            }

            st.lastBucketStart = bucket;
            st.lastAvailability = availability;
            if (svcType != null && !svcType.isEmpty()) {
                st.serviceType = svcType;
            }
            if (svcTier != null && !svcTier.isEmpty()) {
                st.serviceTier = svcTier;
            }
            if (area != null && !area.isEmpty()) {
                st.areaCode = area;
            }
            state.update(st);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricRecord> out) throws Exception {
            if (ctx.timeDomain() != TimeDomain.EVENT_TIME) {
                return;
            }
            DriverStatusState st = state.value();
            if (st == null || st.churned) {
                return;
            }
            Instant eventTime = Instant.ofEpochMilli(timestamp);
            emitMetric(metric("churn_driver", 1d, eventTime, "COUNT", nvl(st.serviceType), nvl(st.serviceTier), nvl(st.areaCode)), out);
            st.churned = true;
            st.churnTimerTs = null;
            state.update(st);
        }
    }

    static class DriverStatusState implements java.io.Serializable {
        Instant lastBucketStart;
        String lastAvailability;
        Instant lastBusyDay;
        Long churnTimerTs;
        boolean churned;
        String serviceType = "";
        String serviceTier = "";
        String areaCode = "";
    }

    static class PassengerActivityState implements java.io.Serializable {
        Instant lastActiveDay;
        Long churnTimerTs;
        boolean churned;
    }

    static class PassengerRegistrationState implements java.io.Serializable {
        boolean created;
    }

    static class DriverRegistrationProcessor extends KeyedProcessFunction<String, DriverEvent, MetricRecord> {
        private transient ValueState<Boolean> createdState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("driver-registration", Boolean.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            createdState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(DriverEvent event, Context ctx, Collector<MetricRecord> out) throws Exception {
            Boolean created = createdState.value();
            if (created == null) created = Boolean.FALSE;

            if ("c".equalsIgnoreCase(event.op) && !created) {
                Instant eventTime = coalesce(event.createdAt, event.updatedAt, Instant.now());
                MetricRecord metric = metric("new_driver", 1d, eventTime, "COUNT",
                        nvl(event.serviceType), nvl(event.serviceTier), nvl(event.areaCode));
                emitMetric(metric, out);
                created = Boolean.TRUE;
            }

            createdState.update(created);
        }
    }

    static class DriverLocationEnrichmentFunction extends BroadcastProcessFunction<DriverLocDoc, DriverEvent, DriverLocDoc> {
        @Override
        public void processElement(DriverLocDoc value, ReadOnlyContext ctx, Collector<DriverLocDoc> out) throws Exception {
            if (value == null || value.driverId == null || value.driverId.isEmpty()) {
                return;
            }
            ReadOnlyBroadcastState<String, DriverProfile> state = ctx.getBroadcastState(DRIVER_PROFILE_BROADCAST_STATE);
            if (state != null) {
                DriverProfile profile = state.get(value.driverId);
                if (profile != null) {
                    if (profile.serviceType != null && !profile.serviceType.isEmpty()) {
                        value.serviceType = profile.serviceType;
                    }
                    if (profile.serviceTier != null && !profile.serviceTier.isEmpty()) {
                        value.serviceTier = profile.serviceTier;
                    }
                    if (profile.areaCode != null && !profile.areaCode.isEmpty()) {
                        value.areaCode = profile.areaCode;
                    }
                }
            }
            out.collect(value);
        }

        @Override
        public void processBroadcastElement(DriverEvent event, Context ctx, Collector<DriverLocDoc> out) throws Exception {
            if (event == null || event.driverId == null || event.driverId.isEmpty()) {
                return;
            }
            BroadcastState<String, DriverProfile> state = ctx.getBroadcastState(DRIVER_PROFILE_BROADCAST_STATE);
            DriverProfile profile = state.get(event.driverId);
            if (profile == null) {
                profile = new DriverProfile();
            }
            if (event.serviceType != null && !event.serviceType.isEmpty()) {
                profile.serviceType = event.serviceType;
            }
            if (event.serviceTier != null && !event.serviceTier.isEmpty()) {
                profile.serviceTier = event.serviceTier;
            }
            if (event.areaCode != null && !event.areaCode.isEmpty()) {
                profile.areaCode = event.areaCode;
            }
            state.put(event.driverId, profile);
        }
    }

    static class MetricAggregate implements java.io.Serializable {
        double metricValue;
        double numeratorValue;
        double denominatorValue;
        double sampleSize;
        Instant firstEventAt;
        Instant lastEventAt;
        Instant processedAt;
        int maxProcessingLagMs;
        Long clearTimerTs;
    }

    static class MetricBucketAggregator extends KeyedProcessFunction<String, MetricRecord, MetricRecord> {
        private transient ValueState<MetricAggregate> aggregateState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<MetricAggregate> descriptor =
                    new ValueStateDescriptor<>("metric-aggregate", MetricAggregate.class);
            descriptor.enableTimeToLive(STATE_TTL_30_DAYS);
            aggregateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(MetricRecord value, Context ctx, Collector<MetricRecord> out) throws Exception {
            if (value == null) {
                return;
            }
            MetricAggregate aggregate = aggregateState.value();
            if (aggregate == null) {
                aggregate = new MetricAggregate();
            }

            aggregate.metricValue += value.metricValue;
            aggregate.numeratorValue += value.numeratorValue;
            aggregate.denominatorValue += value.denominatorValue;
            aggregate.sampleSize += value.sampleSize;
            aggregate.firstEventAt = minInstant(aggregate.firstEventAt, value.firstEventAt);
            aggregate.lastEventAt = maxInstant(aggregate.lastEventAt, value.lastEventAt);
            aggregate.processedAt = maxInstant(aggregate.processedAt, value.processedAt);
            aggregate.maxProcessingLagMs = Math.max(aggregate.maxProcessingLagMs, value.processingLagMs);

            aggregateState.update(aggregate);

            MetricRecord result = copyMetric(value);
            result.metricValue = recomputeMetricValue(value.metricName, aggregate.metricValue,
                    aggregate.numeratorValue, aggregate.denominatorValue);
            result.numeratorValue = aggregate.numeratorValue;
            result.denominatorValue = aggregate.denominatorValue;
            result.sampleSize = aggregate.sampleSize;
            result.firstEventAt = aggregate.firstEventAt != null ? aggregate.firstEventAt : value.firstEventAt;
            result.lastEventAt = aggregate.lastEventAt != null ? aggregate.lastEventAt : value.lastEventAt;
            result.processedAt = aggregate.processedAt != null ? aggregate.processedAt : value.processedAt;
            result.processingLagMs = aggregate.maxProcessingLagMs;
            out.collect(result);

            Instant clearAt = bucketEnd(value.bucketStart, value.bucketGranularity);
            if (clearAt != null) {
                long ts = clearAt.plus(MAX_OUT_OF_ORDERNESS).toEpochMilli();
                ctx.timerService().registerEventTimeTimer(ts);
                aggregate.clearTimerTs = ts;
                aggregateState.update(aggregate);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricRecord> out) throws Exception {
            if (ctx.timeDomain() != TimeDomain.EVENT_TIME) {
                return;
            }
            MetricAggregate aggregate = aggregateState.value();
            if (aggregate != null && aggregate.clearTimerTs != null && aggregate.clearTimerTs == timestamp) {
                aggregateState.clear();
            }
        }
    }

    // ==== Helpers ==== //
    static double recomputeMetricValue(String metricName, double aggregated, double numerator, double denominator) {
        if (metricName == null) {
            return aggregated;
        }
        String name = metricName.toLowerCase(Locale.ROOT);
        boolean isTotal = name.endsWith("_total");
        boolean looksLikeRatio = name.contains("rate") || name.contains("_ratio") || name.contains("productivity");
        boolean looksLikeAvgDuration = name.endsWith("_time");

        if (!isTotal && denominator > 0d && (looksLikeRatio || looksLikeAvgDuration)) {
            return numerator / denominator;
        }
        return aggregated;
    }
    static String nvl(String s) { return s == null ? "" : s; }
    static double safe(Double d) { return d == null ? 0.0 : d; }
    static <T> void configureElasticAuth(Elasticsearch7SinkBuilder<T> builder) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES_USERNAME, ES_PASSWORD));
        builder.setRestClientFactory(restClientBuilder -> restClientBuilder
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)));
    }
    static OffsetsInitializer offsetsInitializer(String envVar, String defaultMode) {
        String configured = envVar != null ? System.getenv(envVar) : null;
        String resolved = (configured == null || configured.trim().isEmpty())
                ? defaultMode
                : configured.trim();
        String normalized = resolved.toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "EARLIEST":
                System.out.printf("[%s] Using EARLIEST offsets\n", envVar);
                return OffsetsInitializer.earliest();
            case "LATEST":
                System.out.printf("[%s] Using LATEST offsets\n", envVar);
                return OffsetsInitializer.latest();
            default:
                if (normalized.startsWith("TIMESTAMP:")) {
                    try {
                        long ts = Long.parseLong(normalized.substring("TIMESTAMP:".length()));
                        System.out.printf("[%s] Using TIMESTAMP offsets at %d\n", envVar, ts);
                        return OffsetsInitializer.timestamp(ts);
                    } catch (NumberFormatException ignored) {
                        System.out.printf("[%s] Invalid timestamp '%s', fallback to %s\n", envVar, resolved, defaultMode);
                    }
                } else {
                    System.out.printf("[%s] Unknown offset mode '%s', fallback to %s\n", envVar, resolved, defaultMode);
                }
                return defaultMode.equalsIgnoreCase("EARLIEST")
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest();
        }
    }
    static String metricKey(MetricRecord r) {
        if (r == null) {
            return "";
        }
        String bucket = r.bucketStart != null ? r.bucketStart.toString() : "";
        return String.join("|",
                nvl(r.metricName),
                nvl(r.bucketGranularity),
                bucket,
                nvl(r.serviceType),
                nvl(r.serviceTier),
                nvl(r.areaCode),
                nvl(r.cohortKey),
                nvl(r.entityType),
                nvl(r.entityId)
        );
    }
    static Instant minInstant(Instant a, Instant b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isBefore(b) ? a : b;
    }
    static Instant maxInstant(Instant a, Instant b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isAfter(b) ? a : b;
    }
    static Instant truncateToMinute(Instant t) { return truncateToGranularity(t, "MINUTE"); }

    static MetricRecord metric(String metricName, double metricValue, Instant eventTime, String unit,
                               String serviceType, String serviceTier, String areaCode) {
        return metricWithSample(metricName, metricValue, eventTime, unit, serviceType, serviceTier, areaCode,
                metricValue, 0d, 0d);
    }

    static MetricRecord ratioMetric(String metricName, double numerator, double denominator, Instant eventTime,
                                    String unit, String serviceType, String serviceTier, String areaCode) {
        return metricWithSample(metricName, numerator, eventTime, unit, serviceType, serviceTier, areaCode,
                numerator, denominator, 0d);
    }

    static MetricRecord durationMetric(String metricName, double seconds, Instant eventTime,
                                       String serviceType, String serviceTier, String areaCode, double denominator) {
        return metricWithSample(metricName, seconds, eventTime, "SECOND", serviceType, serviceTier, areaCode,
                seconds, denominator, denominator);
    }

    static MetricRecord metricWithSample(String metricName, double metricValue, Instant eventTime, String unit,
                                         String serviceType, String serviceTier, String areaCode,
                                         double numerator, double denominator, double sample) {
        MetricRecord r = new MetricRecord();
        r.metricName = metricName;
        r.serviceType = nvl(serviceType);
        r.serviceTier = nvl(serviceTier);
        r.areaCode = nvl(areaCode);
        Instant processed = Instant.now();
        Instant evt = eventTime != null ? eventTime : processed;
        r.eventTime = evt;
        r.processedAt = processed;
        r.bucketStart = truncateToGranularity(evt, "MINUTE");
        r.metricValue = metricValue;
        r.numeratorValue = numerator;
        r.denominatorValue = denominator;
        r.sampleSize = sample;
        r.metricUnit = unit;
        r.firstEventAt = evt;
        r.lastEventAt = evt;
        long lag = Duration.between(evt, processed).toMillis();
        r.processingLagMs = (int) Math.max(0L, lag);
        return r;
    }

    static void emitPassengerActivity(KeyedProcessFunction<String, BookingEvent, MetricRecord>.Context ctx,
                                      String passengerId,
                                      String type,
                                      Instant eventTime) {
        if (ctx == null || passengerId == null || passengerId.isEmpty() || type == null) {
            return;
        }
        PassengerActivityEvent evt = new PassengerActivityEvent();
        evt.passengerId = passengerId;
        evt.type = type;
        evt.eventTime = eventTime != null ? eventTime : Instant.now();
        ctx.output(PASSENGER_ACTIVITY_TAG, evt);
    }

    static void emitRoute(KeyedProcessFunction<String, BookingEvent, MetricRecord>.Context ctx,
                          BookingEvent event,
                          Instant eventTime) {
        if (ctx == null || event == null || event.bookingId == null || event.bookingId.isEmpty()) {
            return;
        }
        if (event.pickupLat != null && event.pickupLon != null) {
            BookingRouteSegment pickup = new BookingRouteSegment();
            pickup.bookingId = event.bookingId;
            pickup.sequenceNo = 1;
            pickup.latitude = event.pickupLat;
            pickup.longitude = event.pickupLon;
            pickup.eventTime = eventTime;
            ctx.output(BOOKING_ROUTE_TAG, pickup);
        }
        if (event.dropoffLat != null && event.dropoffLon != null) {
            BookingRouteSegment dropoff = new BookingRouteSegment();
            dropoff.bookingId = event.bookingId;
            dropoff.sequenceNo = 2;
            dropoff.latitude = event.dropoffLat;
            dropoff.longitude = event.dropoffLon;
            dropoff.eventTime = eventTime;
            ctx.output(BOOKING_ROUTE_TAG, dropoff);
        }
    }

    static Instant coalesce(Instant... instants) {
        if (instants == null) return null;
        for (Instant inst : instants) {
            if (inst != null) return inst;
        }
        return null;
    }

    static Double secondsBetween(Instant start, Instant end) {
        if (start == null || end == null) return null;
        return Duration.between(start, end).toMillis() / 1000.0;
    }

    static String firstNonEmpty(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }

    static void emitMetric(MetricRecord metric, Collector<MetricRecord> out) {
        for (MetricRecord variant : expandGranularities(metric)) {
            out.collect(variant);
        }
    }

    static List<MetricRecord> expandGranularities(MetricRecord base) {
        List<MetricRecord> variants = new ArrayList<>(5);
        variants.add(base);
        addIfNotNull(variants, cloneForGranularity(base, "HOUR"));
        addIfNotNull(variants, cloneForGranularity(base, "DAY"));
        addIfNotNull(variants, cloneForGranularity(base, "WEEK"));
        addIfNotNull(variants, cloneForGranularity(base, "MONTH"));
        return variants;
    }

    static void addIfNotNull(List<MetricRecord> list, MetricRecord metric) {
        if (metric != null) {
            list.add(metric);
        }
    }

    static MetricRecord cloneForGranularity(MetricRecord base, String granularity) {
        Instant bucketStart = truncateToGranularity(base.eventTime, granularity);
        if (bucketStart == null) {
            return null;
        }
        MetricRecord copy = copyMetric(base);
        copy.bucketGranularity = granularity;
        copy.bucketStart = bucketStart;
        return copy;
    }

    static MetricRecord copyMetric(MetricRecord base) {
        MetricRecord copy = new MetricRecord();
        copy.metricName = base.metricName;
        copy.serviceType = base.serviceType;
        copy.serviceTier = base.serviceTier;
        copy.areaCode = base.areaCode;
        copy.cohortKey = base.cohortKey;
        copy.entityType = base.entityType;
        copy.entityId = base.entityId;
        copy.bucketStart = base.bucketStart;
        copy.bucketGranularity = base.bucketGranularity;
        copy.eventTime = base.eventTime;
        copy.processedAt = base.processedAt;
        copy.metricValue = base.metricValue;
        copy.numeratorValue = base.numeratorValue;
        copy.denominatorValue = base.denominatorValue;
        copy.sampleSize = base.sampleSize;
        copy.processingLagMs = base.processingLagMs;
        copy.firstEventAt = base.firstEventAt;
        copy.lastEventAt = base.lastEventAt;
        copy.metricUnit = base.metricUnit;
        return copy;
    }

    static Instant truncateToGranularity(Instant timestamp, String granularity) {
        if (timestamp == null) {
            return null;
        }
        ZonedDateTime zoned = timestamp.atZone(REPORTING_ZONE_ID);
        switch (granularity) {
            case "HOUR":
                return zoned.truncatedTo(ChronoUnit.HOURS).toInstant();
            case "DAY":
                return zoned.truncatedTo(ChronoUnit.DAYS).toInstant();
            case "WEEK":
                ZonedDateTime week = zoned.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                        .truncatedTo(ChronoUnit.DAYS);
                return week.toInstant();
            case "MONTH":
                ZonedDateTime month = zoned.with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS);
                return month.toInstant();
            case "MINUTE":
            default:
                return zoned.truncatedTo(ChronoUnit.MINUTES).toInstant();
        }
    }

    static Instant bucketEnd(Instant bucketStart, String granularity) {
        if (bucketStart == null) {
            return null;
        }
        ZonedDateTime zoned = bucketStart.atZone(REPORTING_ZONE_ID);
        switch (granularity) {
            case "HOUR":
                return zoned.plusHours(1).toInstant();
            case "DAY":
                return zoned.plusDays(1).toInstant();
            case "WEEK":
                return zoned.plusWeeks(1).toInstant();
            case "MONTH":
                return zoned.plusMonths(1).toInstant();
            case "MINUTE":
            default:
                return zoned.plusMinutes(1).toInstant();
        }
    }

    static boolean isOnline(String availability) {
        if (availability == null) return false;
        switch (availability) {
            case "AVAILABLE":
            case "BUSY":
            case "ONLINE":
                return true;
            default:
                return false;
        }
    }

    static BookingEvent mapBooking(ObjectNode node) {
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) return null;
        BookingEvent e = new BookingEvent();
        e.op            = node.has("op") ? node.get("op").asText() : "u";
        e.bookingId     = getTextCI(after, "id");
        e.status        = getTextCI(after, "status");
        e.passengerId   = getTextCI(after, "passengerId");
        e.serviceType   = getTextCI(after, "serviceTypeCode");
        e.serviceTier   = getTextCI(after, "serviceTierCode");
        e.areaCode      = getTextCI(after, "wardCode");
        e.platformFee   = getDoubleCI(after, "platformFee");
        e.discountAmount= getDoubleCI(after, "discountAmount");
        double[] pickup = extractCoordinates(after, "pickupLocation");
        if (pickup != null) {
            e.pickupLon = pickup[0];
            e.pickupLat = pickup[1];
        }
        double[] dropoff = extractCoordinates(after, "dropoffLocation");
        if (dropoff != null) {
            e.dropoffLon = dropoff[0];
            e.dropoffLat = dropoff[1];
        }
        e.createdAt     = parseInstant(getTextCI(after, "createdAt"));
        e.acceptedAt    = parseInstant(firstNonEmpty(
                getTextCI(after, "acceptedAt"),
                getTextCI(after, "driverAcceptedAt"),
                getTextCI(after, "atPickUpAt")));
        e.startTripAt   = parseInstant(getTextCI(after, "startTripAt"));
        e.completedAt   = parseInstant(getTextCI(after, "completedAt"));
        e.updatedAt     = parseInstant(getTextCI(after, "updatedAt"));
        return e;
    }

    static PassengerEvent mapPassenger(ObjectNode node) {
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) return null;
        PassengerEvent p = new PassengerEvent();
        p.op = node.has("op") ? node.get("op").asText() : "u";
        p.passengerId = getTextCI(after, "id");
        p.createdAt = parseInstant(getTextCI(after, "createdAt"));
        p.updatedAt = parseInstant(getTextCI(after, "updatedAt"));
        return p;
    }

    static DriverEvent mapDriver(ObjectNode node) {
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) return null;
        DriverEvent d = new DriverEvent();
        d.op = node.has("op") ? node.get("op").asText() : "u";
        d.driverId = getTextCI(after, "id");
        d.serviceType = getTextCI(after, "serviceCode");
        d.serviceTier = getTextCI(after, "serviceTierCode");
        d.areaCode = getTextCI(after, "areaCode");
        d.createdAt = parseInstant(getTextCI(after, "createdAt"));
        d.updatedAt = parseInstant(getTextCI(after, "updatedAt"));
        return d;
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
        Instant updated = parseInstant(getTextCI(after, "updatedAt"));
        d.updatedAt = updated != null ? updated : Instant.now();
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
        if (n.isNumber()) {
            return n.asDouble();
        }
        if (n.isTextual()) {
            try {
                return new BigDecimal(n.asText()).doubleValue();
            } catch (NumberFormatException ignore) {
                return 0d;
            }
        }
        return 0d;
    }
    static JsonNode getFieldCI(JsonNode obj, String field) {
        if (obj == null || field == null) {
            return null;
        }
        String trimmed = field.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String snake = toSnake(trimmed);
        String camel = toCamel(trimmed);
        String[] candidates = new String[] {
                trimmed,
                trimmed.toLowerCase(Locale.ROOT),
                trimmed.toUpperCase(Locale.ROOT),
                snake,
                snake != null ? snake.toUpperCase(Locale.ROOT) : null,
                camel,
                camel != null ? camel.toUpperCase(Locale.ROOT) : null
        };
        for (String candidate : candidates) {
            if (candidate != null && obj.has(candidate)) {
                return obj.get(candidate);
            }
        }
        return null;
    }

    static String toSnake(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String withUnderscore = value.replace('-', '_');
        return withUnderscore
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .toLowerCase(Locale.ROOT);
    }

    static String toCamel(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String[] parts = value.split("_");
        if (parts.length == 0) {
            return value;
        }
        StringBuilder builder = new StringBuilder(parts[0].toLowerCase(Locale.ROOT));
        for (int i = 1; i < parts.length; i++) {
            if (parts[i].isEmpty()) {
                continue;
            }
            builder.append(parts[i].substring(0, 1).toUpperCase(Locale.ROOT));
            if (parts[i].length() > 1) {
                builder.append(parts[i].substring(1).toLowerCase(Locale.ROOT));
            }
        }
        return builder.toString();
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

    static double[] extractCoordinates(JsonNode obj, String field) {
        JsonNode node = getFieldCI(obj, field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.has("latitude") && node.has("longitude")) {
            return new double[] { node.get("longitude").asDouble(), node.get("latitude").asDouble() };
        }
        try {
            if (node.has("wkb") && !node.get("wkb").isNull()) {
                byte[] bytes = java.util.Base64.getDecoder().decode(node.get("wkb").asText());
                org.locationtech.jts.geom.Point p = (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(bytes);
                return new double[] { p.getX(), p.getY() };
            }
            if (node.isTextual()) {
                String value = node.asText();
                try {
                    byte[] bytes = java.util.Base64.getDecoder().decode(value);
                    org.locationtech.jts.geom.Point p = (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(bytes);
                    return new double[] { p.getX(), p.getY() };
                } catch (IllegalArgumentException ex) {
                    byte[] hex = hexStringToByteArray(value);
                    org.locationtech.jts.geom.Point p = (org.locationtech.jts.geom.Point) new org.locationtech.jts.io.WKBReader().read(hex);
                    return new double[] { p.getX(), p.getY() };
                }
            }
        } catch (Exception ignore) {
            return null;
        }
        return null;
    }

    static String resolve(String candidate, String fallback) {
        String c = candidate == null ? "" : candidate;
        if (!c.isEmpty()) {
            return c;
        }
        return fallback == null ? "" : fallback;
    }

    static WatermarkStrategy<ObjectNode> watermarkByUpdatedAt() {
        return WatermarkStrategy
                .<ObjectNode>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, previous) -> {
                    Instant timestamp = extractTimestamp(event, "updatedAt");
                    if (timestamp == null) {
                        timestamp = extractTimestamp(event, "createdAt");
                    }
                    if (timestamp == null) {
                        timestamp = Instant.now();
                    }
                    return timestamp.toEpochMilli();
                });
    }

    static Instant extractTimestamp(ObjectNode node, String field) {
        if (node == null) {
            return null;
        }
        JsonNode after = node.get("after");
        if (after == null || after.isNull()) {
            return null;
        }
        String value = getTextCI(after, field);
        if (value == null) {
            return null;
        }
        return parseInstant(value);
    }
}
