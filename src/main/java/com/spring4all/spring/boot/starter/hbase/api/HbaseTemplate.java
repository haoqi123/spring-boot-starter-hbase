package com.spring4all.spring.boot.starter.hbase.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */

/**
 * JThink@JThink
 *
 * @author JThink
 * @version 0.0.1
 * desc： copy from spring data hadoop hbase, modified by JThink, use the 1.0.0 api
 * date： 2016-11-15 15:42:46
 */
@Slf4j
public class HbaseTemplate implements HbaseOperations {
    private Configuration configuration;

    private volatile Connection connection;

    public HbaseTemplate(Configuration configuration) {
        this.setConfiguration(configuration);
        Assert.notNull(configuration, " a valid configuration is required");
    }

    @Override
    public <T> T execute(String tableName, TableCallback<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
            throw new HbaseSystemException(throwable);
        } finally {
            if (null != table) {
                try {
                    table.close();
                    sw.stop();
                } catch (IOException e) {
                    log.error("hbase资源释放失败");
                }
            }
        }
    }

    @Override
    public <T> List<T> scan(String tableName, String family, final RowMapper<T> action) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addFamily(Bytes.toBytes(family));
        return this.scan(tableName, scan, action);
    }

    @Override
    public <T> List<T> scan(String tableName, String family, String qualifier, final RowMapper<T> action) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return this.scan(tableName, scan, action);
    }

    @Override
    public <T> List<T> scan(String tableName, final Scan scan, final RowMapper<T> action) {
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
                int caching = scan.getCaching();
                // 如果caching未设置(默认是1)，将默认配置成5000
                if (caching == 1) {
                    scan.setCaching(5000);
                }
                try (ResultScanner scanner = table.getScanner(scan)) {
                    List<T> rs = new ArrayList<T>();
                    int rowNum = 0;
                    for (Result result : scanner) {
                        rs.add(action.mapRow(result, rowNum++));
                    }
                    return rs;
                }
            }
        });
    }

    @Override
    public List<byte[]> scan(String tableName, String family, String qualifier) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return this.scan(tableName, scan);
    }

    @Override
    public List<byte[]> scan(String tableName, Scan scan) {
        if (scan.getFamilies().length != 1) {
            throw new IllegalArgumentException("列族只能设置一个");
        }
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        byte[] family = scan.getFamilies()[0];
        NavigableSet<byte[]> navigableSet = familyMap.get(family);
        if (null == navigableSet || navigableSet.size() != 1) {
            throw new IllegalArgumentException("指定列只能设置一个");
        }
        byte[] qualifier = navigableSet.pollFirst();
        return this.execute(tableName, new TableCallback<List<byte[]>>() {
            @Override
            public List<byte[]> doInTable(Table table) throws Throwable {
                int caching = scan.getCaching();
                // 如果caching未设置(默认是1)，将默认配置成5000
                if (caching == 1) {
                    scan.setCaching(5000);
                }
                //TODO NAME-VALUE-FILTER
                try (ResultScanner scanner = table.getScanner(scan)) {
                    List<byte[]> rs = new ArrayList<>();
                    for (Result result : scanner) {
                        rs.add(result.getValue(family, qualifier));
                    }
                    return rs;
                }
            }
        });
    }

    @Override
    public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
        return this.get(tableName, rowName, null, null, mapper);
    }

    @Override
    public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
        return this.get(tableName, rowName, familyName, null, mapper);
    }

    @Override
    public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) {
        return this.execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                if (StringUtils.isNotBlank(familyName)) {
                    byte[] family = Bytes.toBytes(familyName);
                    if (StringUtils.isNotBlank(qualifier)) {
                        get.addColumn(family, Bytes.toBytes(qualifier));
                    } else {
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result, 0);
            }
        });
    }

    @Override
    public byte[] get(String tableName, String rowName, String familyName, String qualifier) {
        return this.execute(tableName, new TableCallback<byte[]>() {
            @Override
            public byte[] doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                byte[] family = Bytes.toBytes(familyName);
                byte[] qualifierByte = Bytes.toBytes(qualifier);
                get.addColumn(family, qualifierByte);
                Result result = table.get(get);
                return result.getValue(family, qualifierByte);
            }
        });
    }

    @Override
    public void execute(String tableName, MutatorCallback action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(3 * 1024 * 1024));
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            sw.stop();
            throw new HbaseSystemException(throwable);
        } finally {
            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                    sw.stop();
                } catch (IOException e) {
                    log.error("hbase mutator资源释放失败");
                }
            }
        }
    }

    @Override
    public void saveOrUpdate(String tableName, final Mutation mutation) {
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutation);
            }
        });
    }

    @Override
    public void saveOrUpdates(String tableName, final List<Mutation> mutations) {
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }

    @Override
    public boolean createNamespace(String namespace) {
        try {
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
            Admin admin = getConnection().getAdmin();
            builder.addConfiguration("creator", "java-api");
            admin.createNamespace(builder.build());
            return true;
        } catch (IOException e) {
            log.error("创建失败:{}", ExceptionUtils.getStackTrace(e));
        }
        return false;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
                                200,
                                Integer.MAX_VALUE,
                                60L,
                                TimeUnit.SECONDS,
                                new SynchronousQueue<Runnable>());
                        // init pool
                        poolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                    } catch (IOException e) {
                        log.error("hbase connection资源池创建失败");
                    }
                }
            }
        }
        return this.connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
