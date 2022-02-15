/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tair.cli.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.net.RedisSslContextFactory;
import com.tair.cli.monitor.Gateway;
import com.tair.cli.util.Strings;

/**
 * @author Baoyi Chen
 */
public class Configure {
    
    private Properties properties;
    
    private Configure() {
        this.properties = new Properties();
        try {
            String path = System.getProperty("conf");
            if (path != null && path.trim().length() != 0) {
                try (InputStream in = new FileInputStream(path)) {
                    properties.load(in);
                }
            } else {
                ClassLoader loader = Configure.class.getClassLoader();
                try (InputStream in = loader.getResourceAsStream("tair-cli.conf")) {
                    properties.load(in);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    private Configure(Properties properties) {
        this();
        if (properties != null)
            this.properties.putAll(properties);
    }
    
    public Properties properties() {
        return this.properties;
    }

    /**
     * rct --format resp batch size
     */
    private int batchSize = 128;
    
    /**
     * timeout
     */
    private int timeout = 60000;
    
    /**
     * metric uri
     */
    private URI metricUri;
    
    /**
     * metric user
     */
    private String metricUser;
    
    /**
     * metric pass
     */
    private String metricPass;
    
    /**
     * metric gateway
     */
    private Gateway metricGateway;
    
    /**
     * metric database
     */
    private String metricDatabase;
    
    /**
     * metric retention policy
     */
    private String metricRetentionPolicy;
    
    /**
     * metric instance
     */
    private String metricInstance;

    /**
     * ssl parameter
     */
    private boolean sourceDefaultTruststore;
    
    /**
     * ssl parameter
     */
    private String sourceKeystorePath;
    
    /**
     * ssl parameter
     */
    private String sourceKeystorePass;
    
    /**
     * ssl parameter
     */
    private String sourceKeystoreType;

    /**
     * ssl parameter
     */
    private boolean targetDefaultTruststore;
    
    /**
     * ssl parameter
     */
    private String targetKeystorePath;

    /**
     * ssl parameter
     */
    private String targetKeystorePass;

    /**
     * ssl parameter
     */
    private String targetKeystoreType;
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public int getTimeout() {
        return timeout;
    }
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    public URI getMetricUri() {
        return metricUri;
    }
    
    public void setMetricUri(URI metricUri) {
        this.metricUri = metricUri;
    }
    
    public String getMetricUser() {
        return metricUser;
    }
    
    public void setMetricUser(String metricUser) {
        this.metricUser = metricUser;
    }
    
    public String getMetricPass() {
        return metricPass;
    }
    
    public void setMetricPass(String metricPass) {
        this.metricPass = metricPass;
    }
    
    public Gateway getMetricGateway() {
        return metricGateway;
    }
    
    public void setMetricGateway(Gateway metricGateway) {
        this.metricGateway = metricGateway;
    }

    public String getMetricDatabase() {
        return metricDatabase;
    }

    public void setMetricDatabase(String metricDatabase) {
        this.metricDatabase = metricDatabase;
    }

    public String getMetricRetentionPolicy() {
        return metricRetentionPolicy;
    }

    public void setMetricRetentionPolicy(String metricRetentionPolicy) {
        this.metricRetentionPolicy = metricRetentionPolicy;
    }

    public String getMetricInstance() {
        return metricInstance;
    }

    public void setMetricInstance(String metricInstance) {
        this.metricInstance = metricInstance;
    }

    public String getSourceKeystorePath() {
        return sourceKeystorePath;
    }

    public void setSourceKeystorePath(String sourceKeystorePath) {
        this.sourceKeystorePath = sourceKeystorePath;
    }

    public String getSourceKeystorePass() {
        return sourceKeystorePass;
    }

    public void setSourceKeystorePass(String sourceKeystorePass) {
        this.sourceKeystorePass = sourceKeystorePass;
    }

    public String getSourceKeystoreType() {
        return sourceKeystoreType;
    }

    public void setSourceKeystoreType(String sourceKeystoreType) {
        this.sourceKeystoreType = sourceKeystoreType;
    }

    public String getTargetKeystorePath() {
        return targetKeystorePath;
    }

    public void setTargetKeystorePath(String targetKeystorePath) {
        this.targetKeystorePath = targetKeystorePath;
    }

    public String getTargetKeystorePass() {
        return targetKeystorePass;
    }

    public void setTargetKeystorePass(String targetKeystorePass) {
        this.targetKeystorePass = targetKeystorePass;
    }

    public String getTargetKeystoreType() {
        return targetKeystoreType;
    }

    public void setTargetKeystoreType(String targetKeystoreType) {
        this.targetKeystoreType = targetKeystoreType;
    }

    public boolean isSourceDefaultTruststore() {
        return sourceDefaultTruststore;
    }

    public void setSourceDefaultTruststore(boolean sourceDefaultTruststore) {
        this.sourceDefaultTruststore = sourceDefaultTruststore;
    }

    public boolean isTargetDefaultTruststore() {
        return targetDefaultTruststore;
    }

    public void setTargetDefaultTruststore(boolean targetDefaultTruststore) {
        this.targetDefaultTruststore = targetDefaultTruststore;
    }

    public Configuration merge(RedisURI uri, boolean source) {
        Configuration v = merge(Configuration.valueOf(uri), source);
        return v;
    }

    public Configuration merge(Configuration conf, boolean source) {
        conf.setConnectionTimeout(this.timeout);
        conf.setReadTimeout(this.timeout);

        if (conf.isSsl()) {
            RedisSslContextFactory factory = new RedisSslContextFactory();
            if (source) {
                if (sourceKeystorePath != null) {
                    factory.setKeyStorePath(sourceKeystorePath);
                    factory.setKeyStorePassword(sourceKeystorePass);
                    factory.setKeyStoreType(sourceKeystoreType);
                    if (!sourceDefaultTruststore) {
                        factory.setTrustStorePath(sourceKeystorePath);
                        factory.setTrustStorePassword(sourceKeystorePass);
                        factory.setTrustStoreType(sourceKeystoreType);
                    }
                    
                }
            } else {
                if (targetKeystorePath != null) {
                    factory.setKeyStorePath(targetKeystorePath);
                    factory.setKeyStorePassword(targetKeystorePass);
                    factory.setKeyStoreType(targetKeystoreType);
                    if (!targetDefaultTruststore) {
                        factory.setTrustStorePath(targetKeystorePath);
                        factory.setTrustStorePassword(targetKeystorePass);
                        factory.setTrustStoreType(targetKeystoreType);
                    }
                }
            }
            conf.setSslContextFactory(factory);
        }
        return conf;
    }
    
    public static Configure bind() {
        return bind(null);
    }
    
    public static Configure bind(Properties properties) {
        Configure conf = new Configure(properties);
        conf.batchSize = getInt(conf, "batch_size", 128, true);
        conf.timeout = getInt(conf, "timeout", 60000, true);
        conf.metricUser = getString(conf, "metric_user", "tair_cli", true);
        conf.metricPass = getString(conf, "metric_pass", "tair_cli", true);
        conf.metricUri = getUri(conf, "metric_uri", "http://localhost:8086", true);
        conf.metricGateway = Gateway.parse(getString(conf, "metric_gateway", "none", true));
        conf.metricDatabase = getString(conf, "metric_database", "tair_cli", true);
        conf.metricRetentionPolicy = getString(conf, "metric_retention_policy", "30days", true);
        conf.metricInstance = getString(conf, "metric_instance", "instance0", true);
        
        // ssl
        conf.sourceKeystorePath = getString(conf, "source_keystore_path", null, true);
        conf.sourceKeystorePass = getString(conf, "source_keystore_pass", null, true);
        conf.sourceKeystoreType = getString(conf, "source_keystore_type", "pkcs12", true);
        conf.sourceDefaultTruststore = getBool(conf, "source_default_truststore", false, true);

        conf.targetKeystorePath = getString(conf, "target_keystore_path", null, true);
        conf.targetKeystorePass = getString(conf, "target_keystore_pass", null, true);
        conf.targetKeystoreType = getString(conf, "target_keystore_type", "pkcs12", true);
        conf.targetDefaultTruststore = getBool(conf, "target_default_truststore", false, true);
        return conf;
    }
    
    public static boolean getBool(String value, boolean defaultValue) {
        if (value == null)
            return defaultValue;
        if (value.equals("false") || value.equals("no"))
            return false;
        if (value.equals("true") || value.equals("yes"))
            return true;
        return defaultValue;
    }

    public static int getInt(String value, int defaultValue) {
        if (value == null)
            return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static long getLong(String value, long defaultValue) {
        if (value == null)
            return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    public static URI getUri(Configure conf, String key) {
        return getUri(conf, key, null, false);
    }
    
    public static String getString(Configure conf, String key) {
        return getString(conf, key, null, false);
    }
    
    public static Integer getInt(Configure conf, String key) {
        return getInt(conf, key, null, false);
    }
    
    public static Boolean getBool(Configure conf, String key) {
        return getBool(conf, key, null, false);
    }

    public static List<String> getList(Configure conf, String key) {
        return getList(conf, key, null, false);
    }
    
    public static URI getUri(Configure conf, String key, String value, boolean optional) {
        String v = getString(conf, key, value, optional);
        try {
            return v == null ? null : new URI(v);
        } catch (Throwable e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
    
    public static String getString(Configure conf, String key, String value, boolean optional) {
        String v = System.getProperty(key);
        if (Strings.isEmpty(v) && Strings.isEmpty(v = conf.properties.getProperty(key)))
            v = value;
        if (v == null && !optional) {
            throw new IllegalArgumentException("not found the config[key=" + key + "]");
        }
        return v;
    }
    
    public static Integer getInt(Configure conf, String key, Integer value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value.toString(), optional);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not found the config[key=" + key + "]");
        }
    }
    
    public static Boolean getBool(Configure conf, String key, Boolean value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value.toString(), optional);
        if (v == null)
            return value;
        if (v.equals("yes") || v.equals("true"))
            return Boolean.TRUE;
        if (v.equals("no") || v.equals("false"))
            return Boolean.FALSE;
        throw new IllegalArgumentException("not found the config[key=" + key + "]");
    }

    public static List<String> getList(Configure conf, String key, String value, boolean optional) {
        String v = getString(conf, key, value == null ? null : value, optional);
        if (v == null) 
            return null;
        return Arrays.stream(v.split(",")).map(e -> e.trim()).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "Configure{" +
                "batchSize=" + batchSize +
                ", timeout=" + timeout +
                ", metricUri=" + metricUri +
                ", metricUser='" + metricUser + '\'' +
                ", metricPass='" + metricPass + '\'' +
                ", metricGateway=" + metricGateway +
                ", metricDatabase='" + metricDatabase + '\'' +
                ", metricRetentionPolicy='" + metricRetentionPolicy + '\'' +
                ", metricInstance='" + metricInstance + '\'' +
                ", sourceDefaultTruststore=" + sourceDefaultTruststore +
                ", sourceKeystorePath='" + sourceKeystorePath + '\'' +
                ", sourceKeystorePass='" + sourceKeystorePass + '\'' +
                ", sourceKeystoreType='" + sourceKeystoreType + '\'' +
                ", targetDefaultTruststore=" + targetDefaultTruststore +
                ", targetKeystorePath='" + targetKeystorePath + '\'' +
                ", targetKeystorePass='" + targetKeystorePass + '\'' +
                ", targetKeystoreType='" + targetKeystoreType + '\'' +
                '}';
    }
}
