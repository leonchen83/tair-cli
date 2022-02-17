package com.tair.cli.monitor.gateway.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdb.BatchOptions.DEFAULTS;
import static org.influxdb.InfluxDB.ConsistencyLevel.ONE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tair.cli.conf.Configure;
import com.tair.cli.monitor.MonitorPoint;
import com.tair.cli.monitor.gateway.MetricGateway;
import com.tair.cli.util.XThreadFactory;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

/**
 * @author Baoyi Chen
 */
public class InfluxdbGateway implements MetricGateway {
    private static final Logger logger = LoggerFactory.getLogger(InfluxdbGateway.class);

    public static final String AVG = "avg";
    public static final String TYPE = "type";
    public static final String VALUE = "value";
    public static final String MODULE = "module";
    public static final String FACADE = "facade";
    public static final String INSTANCE = "instance";

    protected int port;
    protected int threads = 1;
    protected String instance;
    protected InfluxDB influxdb;
    protected int actions = 256;
    protected int jitter = 1000;
    protected int interval = 2000;
    protected int capacity = 8192;
    protected Configure configure;
    protected String retention = "autogen";
    protected ConsistencyLevel consistency = ONE;
    protected String url, database, user, password;

    public InfluxdbGateway(Configure configure) {
        this.configure = configure;
        this.user = configure.getMetricUser();
        this.password = configure.getMetricPass();
        this.instance = configure.properties().getProperty("instance");
        this.url = configure.getMetricUri().toString();
        this.database = configure.getMetricDatabase();
        this.retention = configure.getMetricRetentionPolicy();
        this.influxdb = create();
    }
    
    @Override
    public void reset(String measurement) {
        if (this.influxdb != null) {
            this.influxdb.query(new Query("drop series from \"" + measurement + "\" where instance = '" + instance + "'", database));
        }
    }

    @Override
    public void close() throws IOException {
        if (this.influxdb != null) this.influxdb.close();
    }

    @Override
    public boolean save(List<MonitorPoint> points) {
        //
        if (points.isEmpty()) {
            return false;
        }

        //
        try {
            for (Point p : toPoints(points)) influxdb.write(p);
            return true;
        } catch (Throwable t) {
            logger.error("failed to save points.", t);
            return false;
        }
    }

    protected List<Point> toPoints(List<MonitorPoint> points) {
        final List<Point> r = new ArrayList<>((points.size()));
        for (MonitorPoint point : points) r.add(toPoint(point));
        return r;
    }

    protected Point toPoint(final MonitorPoint point) {
        //
        final String name = point.getMonitorName();
        final String facade = point.getMonitorKey();
        final int index = name.indexOf('_');
        String module = index > 0 ? name.substring(0, index) : name;
        double avg = 0d;
        if (point.getTime() != 0L && point.getValue() != 0L) avg = point.getTime() / 1000000d / point.getValue();

        //
        return Point.measurement(name).time(point.getTimestamp(), MILLISECONDS).addField(VALUE, point.getValue()).addField(AVG, avg)
                .tag(MODULE, module).tag(TYPE, point.getMonitorType().name()).tag(FACADE, facade).tag(INSTANCE, instance).build();
    }

    public class ExceptionHandler implements BiConsumer<Iterable<Point>, Throwable> {
        @Override
        public void accept(final Iterable<Point> points, final Throwable t) {
            logger.warn("failed to save points.", t);
        }
    }

    protected InfluxDB create() {
        //
        final OkHttpClient.Builder http = new OkHttpClient.Builder();
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(2); dispatcher.setMaxRequestsPerHost(2);
        http.dispatcher(dispatcher);
        http.connectionPool(new ConnectionPool(threads, 5, TimeUnit.MINUTES));
        
        //
        final InfluxDB r = InfluxDBFactory.connect(url, user, password, http);
        BatchOptions opt = DEFAULTS;
        opt = opt.consistency(consistency).jitterDuration(jitter);
        opt = opt.actions(this.actions).threadFactory(new XThreadFactory("influxdb"));
        opt = opt.exceptionHandler(new ExceptionHandler()).bufferLimit(capacity).flushDuration(interval);
        r.setDatabase(this.database).setRetentionPolicy(retention).enableBatch((opt)).enableGzip();
        return r;
    }
}
