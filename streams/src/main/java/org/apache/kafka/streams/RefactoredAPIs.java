package org.apache.kafka.streams;

/* --- Following classes are for secondary class upgrades --- */

interface Buffered<K, V1, V2> {      // it is for potential modifications for stream-stream join impl

    static Buffered<K, V1, V2>				    withKeySerde(Serde<K>);

    static Buffered<K, V1, V2>				    withLeftValueSerde(Serde<V1>);

    static Buffered<K, V1, V2>				    withRightValueSerde(Serde<V2>);

    Buffered<K, V1, V2>    				        withKeySerde(Serde<K>);

    Buffered<K, V1, V2>    				        withLeftValueSerde(Serde<V1>);

    Buffered<K, V1, V2>    				        withRightValueSerde(Serde<V2>);
}

interface Materialized<K, V> {

    static Materialized<K, V>				    as(String);		// queryable name

    static Materialized<K, V>				    as(StateStoreSupplier);		// queryable name in StateStoreSupplier.name()

    Materialized<K, V>	    					withKeySerde(Serde<K>);

    Materialized<K, V>				    		withValueSerde(Serde<V>);

    Materialized<K, V>  						withLoggingEnabled(Map<String, String>);    // if it is from source KTable then ignore this

    Materialized<K, V>	    					withLoggingDisabled();                      // if it is from source KTable then ignore this

    Materialized<K, V>		    				withCachingEnabled();

    Materialized<K, V>			    			withCachingDisabled();
}

interface Loaded<K, V> extends Materialized<K, V> {

    static Loaded<K, V>				            as(String);

    static Loaded<K, V>				            as(StateStoreSupplier);

    Loaded<K, V>                                withTimestampExtractor(TimestampExtractor);

    Loaded<K, V>                                withAutoOffsetReset(AutoOffsetReset);
}

interface Serialized<K, V> {

    static Serialized<K, V>				        withKeySerde(Serde<K>);

    static Serialized<K, V>				        withValueSerde(Serde<V>);

    Serialized<K, V>    				        withKeySerde(Serde<K>);

    Serialized<K, V>    				        withValueSerde(Serde<V>);
}

interface Consumed<K, V> extends Serialized<K, V> {

    static Consumed<K, V>                       withTimestampExtractor(TimestampExtractor);

    static Consumed<K, V>                       withAutoOffsetReset(AutoOffsetReset);

    Consumed<K, V>                              withTimestampExtractor(TimestampExtractor);

    Consumed<K, V>                              withAutoOffsetReset(AutoOffsetReset);
}

class StreamsBuilder {

    // we get rid of the multiple topic subscription and encourage users to use merge()

    SourcedKStream<K, V>						from(Pattern);              // regex

    KSource<K, V>							    from(String);               // single topic
}

interface KSource<K, V> {

    SourcedKStream<K, V>                        and(String);                // add more topics to this source, can only be KStream now

    KStream<K, V> 								stream();

    KStream<K, V> 								stream(Consumed<K, V>);

    KTable<K, V> 								table();                    // do not materialize this table unless it is involved in join downwards

    KTable<K, V> 								table(Serialized<K, V>);    // do not materialize this table unless it is involved in join downwards

    KTable<K, V> 								table(Loaded<K, V>);

    GlobalKTable<K, V>                          globalTable();              // silently materialize the global table

    GlobalKTable<K, V>                          globalTable(Serialized<K, V>);  // silently materialize the global table

    GlobalKTable<K, V>                          globalTable(Loaded<K, V>);
}

interface SourcedKStream<K, V> {

    SourcedKStream<K, V>                        and(String);

    KStream<K, V> 								stream();

    KStream<K, V>                               stream(Consumed<K, V>);
}


/* --- Following classes are for aggregation operators --- */

interface KStream<K, V> {

    // remove the serde parameters for most of the following APIs ..

    GroupedKStream<K, V>						groupByKey();

    GroupedKStream<KR, V>						groupBy(KeyValueMapper);                // use default serdes in config

    GroupedKStream<KR, V>						groupBy(KeyValueMapper, Serialized<K, V>);

    KStream<KR, VR>                             map(KeyValueMapper);

    KStream<KR, VR>                             flatMap(KeyValueMapper);

    KStream<K, VR>                              mapValues(KeyValueMapper);

    KStream<K, VR>                              flatMapValues(KeyValueMapper);

    KStream<KR, V>                              selectKey(KeyValueMapper);

    // .. to be continued at line 168
}

interface GroupedKStream<K, V> {

    // this is to maintain lambda expression ..

    AggregatedKStream<K, V, AGG>				aggregate(Initializer, Aggregator);

    AggregatedKStream<Windowed<K>, V, AGG>		aggregate(Initializer, Aggregator, Windows);

    AggregatedKStream<K, V, AGG>				reduce(Reducer);

    AggregatedKStream<Windowed<K>, V, AGG>		reduce(Reducer, Windows);

    AggregatedKStream<K, V, AGG>				count();

    AggregatedKStream<Windowed<K>, V, AGG>		count(Windows);
}

interface AggregatedKStream<K, V, AGG> {

    KTable<K, AGG>								table();			            // the aggregated table will be materialized "silently" and cannot be queried

    KTable<K, AGG>								table(Materialized<K, AGG>);
}


/* --- Following classes are for table-table join operators --- */

interface KTable<K, V> {

    JoinedKTable<K, VR>							join(KTable<K, V0>, ValueJoiner);

    JoinedKTable<K, VR>							leftJoin(KTable<K, V0>, ValueJoiner);
}

interface CogroupedKStream<K, V> {

    JoinedKTable<K, V> join(Initializer<V> initializer);                        // rename "aggregate" in KIP-150 to "join"?

    JoinedKTable<Windowed<K>, V> join(Initializer<V> initializer, Windows);
}

interface JoinedKTable<K, V> {

    KTable<K, V>								table();		// in this case the joined table will not be materialized to state store unless it is involved in join downwards

    KTable<K, V>								table(Materialized<K, V>);
}

/* --- Following classes are for stream-table or stream-stream join operators (stream-globaltable join does not matter) --- */

interface KStream<K, V> {

    // .. continued from line 115

    StreamJoinedKStream<K, V, V0, VR>           join(KStream<K, VO>, ValueJoiner, JoinWindows);

    StreamJoinedKStream<K, V, V0, VR>           leftJoin(KStream<K, VO>, ValueJoiner, JoinWindows);

    StreamJoinedKStream<K, V, V0, VR>           outerJoin(KStream<K, VO>, ValueJoiner, JoinWindows);

    TableJoinedKStream<K, V, VR>                join(KTable<K, VT> table, ValueJoiner);

    TableJoinedKStream<K, V, VR>                leftJoin(KTable<K, VT> table, ValueJoiner);

    // .. to be continued at line 168
}

interface StreamJoinedKStream<K, V1, V2, VR> {

    KTable<K, VR>								stream();		// use the default serdes from config for materializing the joining stream(s)

    KTable<K, VR>								stream(Buffered<K, V1, V2>);      // when materializing / serializing the store for stream join, never log / cache, also use an internal store name
}

interface TableJoinedKStream<K, V, VR> {

    KTable<K, VR>								stream();		// use the default serdes from config for serializing the repartitioned stream(s)

    KTable<K, VR>								stream(Serialized<K, V>);
}

/* ------- DSL Examples -------- */

KStream stream1 = builder.from("topic1").and("topic2")
        .stream(Consumed.withKeySerde(..)
                        .withValueSerde(..)
                        .withTimestampExtractor(tsExtractor)
                        .withAutoOffsetReset(AutoOffsetReset.LATEST));  // define all specs

KStream table1 = builder.from("topic1")
        .table(Loaded.as("state1")
                     .withKeySerde(..)
                     .withValueSerde(..)
                     .withCachingEnabled()
                     .withTimestampExtractor(tsExtractor)
                     .withAutoOffsetReset(AutoOffsetReset.EARLIEST));   // define all specs

KTable table2 = stream1.groupBy(mapper, Serialized.withKeySerde(..).withValueSerde(..)).aggregate(init1, agg1)
        .table(Materialized.as("state2").withCachingDisabled());    // just disable caching

KTable table3 = stream1.groupByKey().aggregate(init2, agg2)
        .table(Materialized.as(new myStoreSupplier("state3")));     // use a different store engine

KTable table4 = stream1.groupByKey().aggregate(init2, agg2)
        .table(Materialized.as("state4").withLoggingEnabled(myConfigs));     // overwrite changelog topic configs

KTable table5 = builder.from("topic1").table(Serialized.withKeySerde(..).withValueSerde(..));     // do not materialize the source table

table4.join(table5, joiner).table();            // do not materialize the result table, but because table4 is not involved in the join we need to silently materialize it now

stream1.selectKey(mapper).join(table2, joiner)
        .stream(Serialized.withKeySerde(..).withValueSerde(..));

stream1.map(mapper).leftJoin(stream2, joiner, windows)
        .stream();

/* --- Following classes are for PAPI, I add it here to make the syntax more aligned with the above proposal --- */

// this class is only used for wrapping the provided bare-bone StateStoreSupplier

class StateStoreSupplier<T extends StateStore> {

    // remove logEnabled / loggingConfigs

    String				                        name();

    T				                            get();

    // adding some syntax-sugar for default stores

    static RocksDBKeyValueStoreSupplier<K, V>	RocksDB<K, V>(String name);

    static InMemoryKeyValueStoreSupplier<K, V>	SortedMap<K, V>(String name);

    static LRUKeyValueStoreSupplier<K, V>	    LRUMap<K, V>(String name , long maxEntries);

    static RocksDBWindowStoreSupplier<K, V>	    WindowedRocksDB<K, V>(String name, Windows);
}

interface StateStoreFactory<T extends StateStore> {

    StateStoreFactory<T>	                    withKeySerde(..);

    StateStoreFactory<T>	                    withValueSerde(..);

    StateStoreFactory<T>	                    withLoggingEnabled(Map<String, String>);    // will be ignored if it is linked to source name

    StateStoreFactory<T>	                    withLoggingDisabled();                      // will be ignored if it is linked to source name

    StateStoreFactory<T>	                    withCachingEnabled();	// will be ignored if !store.persistent()

    StateStoreFactory<T>	                    withCachingDisabled();	// will be ignored if !store.persistent()
}

// this internal impl has a build function to return the wrapped store

class StateStoreFactoryImpl<T extends StateStore> {

    // ... internal impl

    T				                            build();
}

class Stores {

    StateStoreFactory<T>		                create(StateStoreSupplier<T> supplier);
}

class TopologyBuilder {

    TopologyBuilder                             addStateStore(StateStoreFactory factory, String... processorNames);

    TopologyBuilder                             addGlobalStore(StateStoreFactory factory, String sourceName, String topic, String processorName, String... processorNames);
}

/* ------- PAPI Examples -------- */

StateStoreFactory store = Stores.create(StateStoreSupplier.RocksDB("store1"))
        .withKeySerde(...)
        .withValueSerde(...)
        .withLoggingEnabled(configs)
        .withCachingEnabled();

topology.addStateStore(store, processOne, processorTwo);






