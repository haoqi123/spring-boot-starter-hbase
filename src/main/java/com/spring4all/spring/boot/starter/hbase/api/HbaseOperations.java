package com.spring4all.spring.boot.starter.hbase.api;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * Interface that specifies a basic set of Hbase operations, implemented by {@link HbaseTemplate}. Not often used,
 * but a useful option to enhance testability, as it can easily be mocked or stubbed.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */

/**
 * JThink@JThink
 *
 * @author JThink
 * @version 0.0.1
 * desc: copy from spring data hadoop hbase, modified by JThink, remove the unuse interface
 * date: 2016-11-15 14:49:52
 */
public interface HbaseOperations {

    /**
     * Executes the given action against the specified table handling resource management.
     * <p>
     * Application exceptions thrown by the action object get propagated to the caller (can only be unchecked).
     * Allows for returning a result object (typically a domain object or collection of domain objects).
     *
     * @param tableName the target table
     * @param <T>       action type
     * @return the result object of the callback action, or null
     */
    <T> T execute(String tableName, TableCallback<T> mapper);

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family    column family
     * @param <T>       action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> scan(String tableName, String family, final RowMapper<T> mapper);

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family    column family
     * @param qualifier column qualifier
     * @param <T>       action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> scan(String tableName, String family, String qualifier, final RowMapper<T> mapper);

    /**
     * Scans the target table using the given {@link Scan} object. Suitable for maximum control over the scanning
     * process.
     * The content is processed row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param scan      table scanner
     * @param <T>       action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> scan(String tableName, final Scan scan, final RowMapper<T> mapper);

    /**
     * 获取指定列的数据，返回byte数组
     *
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
    List<byte[]> scan(String tableName, String family, String qualifier);

    /**
     * 获取指定列的数据，返回byte数组
     * 只能查询单列族，单列名
     */
    List<byte[]> scan(String tableName, final Scan scan);

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName target table
     * @param rowName   row name
     * @param mapper    row mapper
     * @param <T>       mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, String rowName, final RowMapper<T> mapper);

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName  target table
     * @param rowName    row name
     * @param familyName column family
     * @param mapper     row mapper
     * @param <T>        mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper);

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName  target table
     * @param rowName    row name
     * @param familyName family
     * @param qualifier  column qualifier
     * @param mapper     row mapper
     * @param <T>        mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper);

    /**
     * 获取指定列的数据，返回byte数组
     *
     * @param tableName  表名
     * @param rowName    rowkey
     * @param familyName 列族名
     * @param qualifier  列名
     * @return 列值
     */
    byte[] get(String tableName, final String rowName, final String familyName, final String qualifier);

    /**
     * 执行put update or delete
     *
     * @param tableName
     * @param action
     */
    void execute(String tableName, MutatorCallback action);

    /**
     * @param tableName
     * @param mutation
     */
    void saveOrUpdate(String tableName, Mutation mutation);

    /**
     * @param tableName
     * @param mutations
     */
    void saveOrUpdates(String tableName, List<Mutation> mutations);

    /**
     * create namespace
     *
     * @param namespace name
     * @return true/false
     */
    boolean createNamespace(String namespace);
}