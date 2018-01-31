# Suggested Caching Service Change

## Table of Contents
1. [Description](#description)
2. [Findings](#findings)
 * [Java Caching API](#java-caching-api)
 * [Data Storage](#data-storage)
 * [Large Data Sets](#large-data-sets)
3. [Suggestions](#suggestions)
 * [Use Java API](#use-java-api)
 * [Store MetaDataInterface Only Once](#store-metadatainterface-only-once)
 * [Refactor Row Listener](#refactor-rowlistener)

## Description
Currently Pentaho Caching System (PCS) only supports caching by reference. To cache by value
we need to be able to serialize cached objects. This spike should evaluate options for managing
that serialization, ideally without being specific to a particular cache implementation
(ehcache, Hazelcast, etc.)

## Findings
The following sections discuss the current state of the Java Caching API,
how we use it, and some potential issues for large data sets.

### Java Caching API
The Java Caching API (JSR-107) currently supports Store-by-Value by default.

 > The default mechanism, called store-by-value, instructs an implementation to make a copy of
 application provided keys and values prior to storing them in a Cache and later to return a
 new copy of the entries when accessed from a Cache.

We can take advantage of this by simply using the Java Caching API as it is.  In addition in order
for values to be stored by value the objects being stored must implement `Serializable`.

Here is some sample code for creating an instance of the `CacheManager`:

```java
import javax.cache.CacheManager;
import javax.cache.Caching;
...
CachingProvider provider = Caching.getCachingProvider();
CacheManager cacheManager = provider.getCacheManager();
```
When the ehcache jar is included it defaults to the JSR107 supported provider which once again
uses store-by-value.

### Data Storage
Caches are pretty simple.  They store data as a key value pair.  In this case the key is the query
that was run and the object is `CachedService` which contains the a list of `RowMetaAndData` objects
and the number of results.

```
public class RowMetaAndData implements Cloneable {

  private RowMetaInterface rowMeta;
  private Object[] data;
  . . .
}
```

The `RowMetaInterface` object defines the table structure (ie, column types) and the array of data is
the row data in order that maps up to the row meta data.  Currently this is inefficient.  It probably
isn't too bad now if we are using store-by-reference; as a pointer is used to reference a single instance
of the data.  But if we move to store-by-value this will be serialized for each record which is redundant
and will eat up more memory.

### Large Data Sets
As transformations are run there is a row listener that will push data into the cache.  However, since our
storage value is a `CachedService` object which has a list of `RowMetaAndData` objects we are building this
array before storage and then store it in the cache.

```
// ServiceObserver @ Line 70
rowMetaAndData.add(new RowMetaAndData(rowMeta, row));
```

```
// ServiceObserver @ Line 82
CachedService.complete(rowMetaAndData)
```

This can cause memory errors in large data sets.  It would be better to add each row to the cache as it is
going through the listener.

## Suggestions

### Use Java API
The Java Caching API is a standard interface.  Coding to the API allows us to swap out the underlying
implementation by changing the dependency and modifying some configuration properties.  If we upgrade the
ehCache dependency we can trim down the code in the existing wrapper (maybe even get rid of it).

### Store MetaDataInterface Only Once
Let's move the `RowMetaInterface` up a level so we do not store it multiple times.  However, since
the `RowMetaAndData` object is used in other places of the application we will have to mitigate the risk
of refactoring (we may gain efficiencies across the board or we can break things).  For the near term it
may be best to create a new object to use.  We will have to discuss.

### Refactor Row Listener
This section talks about some options we have for storing data in the cache.  The goal is to break up
the data stored so it is not one massive list as the value.  To do this we can cache each row as it is
listened to in the row listener.  In both these options we would have to worry about the eviction
policy.

#### Option #1
We can have to have 2 caches.  One that stores the query and cache prefix and another one that stores
the prefix-key and the row data.  See below:

**Cache Definition**

| Key        | Value       |
| ---------- |-------------|
| 'Select * FROM "Table"' | {prefix: 'abcd', count: 5, metaDataInterface: {...} } |

**Data Records**

| Key    | Value       |
| -------|-------------|
| 'abcd-1' | {firstName: 'first1', lastName: 'last1'} |
| 'abcd-2' | {firstName: 'first2', lastName: 'last2'} |
| 'abcd-3' | {firstName: 'first3', lastName: 'last3'} |
| 'abcd-4' | {firstName: 'first4', lastName: 'last4'} |
| 'abcd-5' | {firstName: 'first5', lastName: 'last5'} |
> Note: JSON Format used to represent the serialized data.

#### Option #2
In this option we push everything up a level.  Instead of creating on cache per service we create a cache
per query.

```
  Cache cache = cacheManager.getCache('queryAbc');
  cache.put(rowKey, rowData);
```
