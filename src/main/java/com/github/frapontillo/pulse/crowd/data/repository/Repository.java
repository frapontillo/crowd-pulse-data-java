/*
 * Copyright 2015 Francesco Pontillo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.frapontillo.pulse.crowd.data.repository;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.mongodb.DBObject;
import com.mongodb.DBObjectCodecProvider;
import com.mongodb.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;

import java.util.Collections;
import java.util.List;

/**
 * Generic repository for MongoDB collections, where:
 * <ul>
 * <li>{@link T} is the class of stored objects</li>
 * <li>{@link K} is the class of the object IDs</li>
 * </ul>
 *
 * @author Francesco Pontillo
 */
public abstract class Repository<T, K> extends HonestDAO<T, K> {
    private MongoCollection<DBObject> collection;
    protected Morphia morphia;
    private Datastore datastore;
    private MongoDatabase rxDatastore;

    private final static CodecRegistry registry = CodecRegistries .fromRegistries(
            com.mongodb.async.client.MongoClients.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(new DBObjectCodecProvider()));

    /**
     * Create a new Repository using the default configuration in `database.properties`.
     */
    protected Repository() {
        this(null);
    }

    /**
     * Create a new Repository using the default configuration in `database.properties` and
     * overriding the db name
     * with the one in input.
     *
     * @param db The database name to use for this Repository instance.
     */
    @SuppressWarnings({"unchecked", "deprecation"}) public Repository(String db) {
        DBConfig config = new DBConfig(getClass(), db);

        MongoClient client = new MongoClient(config.getServerAddress(), config.getCredentials());

        // map all Morphia classes
        morphia = new Morphia();
        morphia.mapPackageFromClass(Message.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .hosts(Collections.singletonList(config.getServerAddress())).build();
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .clusterSettings(clusterSettings)
                        .credentialList(config.getCredentials())
                        .codecRegistry(registry)
                        .build();
        com.mongodb.reactivestreams.client.MongoClient rxClient = MongoClients.create(settings);

        // create and/or get the datastore
        datastore = morphia.createDatastore(client, config.getDBName());
        // init the DAO
        initDAO(datastore);
        ensureIndexes();

        // create the reactive database
        rxDatastore = rxClient.getDatabase(config.getDBName());

    }

    public abstract String getCollectionName();

    public abstract Class<T> getMappedClass();

    public MongoCollection<DBObject> getRxCollection() {
        if (collection == null) {
            collection = rxDatastore.getCollection(getCollectionName(), DBObject.class);
        }
        return collection;
    }

    /**
     * Find an element by its ID.
     *
     * @param id The ID of the element to find.
     *
     * @return The found element or {@code null}.
     */
    public T findById(K id) {
        Query<T> query = createQuery().field("_id").equal(id);
        return findOne(query);
    }

    /**
     * Create a {@link Query} to retrieve elements between two {@link K} representations of object
     * IDs.
     *
     * @param from The ID to start retrieve elements from.
     * @param to   The Id to stop retrieve elements at.
     *
     * @return A {@link Query} to retrieve elements between two IDs.
     */
    public Query<T> findBetweenKeys(K from, K to) {
        Query<T> query = createQuery();
        if (from != null) {
            query.field("_id").greaterThanOrEq(from);
        }
        if (to != null) {
            query.field("_id").lessThanOrEq(to);
        }
        return query;
    }

    /**
     * Get all the elements of the collection as an {@link Observable}<{@link T}>.
     *
     * @return {@link Observable}<{@link T}> that will emit all of the objects in the Collection.
     */
    public Observable<T> get() {
        return Observable.from(findBetweenKeys(null, null).fetch());
    }

    /**
     * Get the elements of the collection with IDs between {@code from} and {@code to} as a {@link
     * List}<{@link T}>.
     *
     * @param from The ID to start retrieve elements from.
     * @param to   The Id to stop retrieve elements at.
     *
     * @return {@link List}<{@link T}> that will contain all of the objects in the Collection.
     */
    public List<T> getBetweenKeys(K from, K to) {
        return findBetweenKeys(from, to).asList();
    }

    /**
     * Returns all elements in the collection returning an {@link Observable} that will handle
     * backpressure.
     *
     * @return {@link Observable} of elements returned by the collection.
     */
    public Observable<T> findRx() {
        return findRx(null);
    }

    /**
     * Executes a filter on the collection returning an {@link Observable} that will handle
     * backpressure.
     *
     * @param filter The {@link Bson} filter to apply to the collection.
     * @return {@link Observable} of elements returned by the collection.
     */
    public Observable<T> findRx(Bson filter) {
        FindPublisher<DBObject> findPublisher;
        if (filter == null) {
            findPublisher = getRxCollection().find();
        } else {
            findPublisher = getRxCollection().find(filter);
        }
        return RxReactiveStreams.toObservable(findPublisher)
                .lift((Observable.Operator<T, DBObject>) subscriber -> new
                        Subscriber<DBObject>() {
                            @Override public void onCompleted() {
                                subscriber.onCompleted();
                            }

                            @Override public void onError(Throwable e) {
                                e.printStackTrace();
                                subscriber.onError(e);
                            }

                            @Override public void onNext(DBObject dbObject) {
                                T object = morphia.fromDBObject(getMappedClass(), dbObject);
                                subscriber.onNext(object);
                            }
                        });
    }
}
