package io.confluent.streaming.kv;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.kv.internals.MeteredKeyValueStore;
import io.confluent.streaming.kv.internals.RestoreFunc;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory key-value store based on a TreeMap
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class InMemoryKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> {

    public InMemoryKeyValueStore(String name, KStreamContext context) {
        super(name, "kafka-streams", new MemoryStore<K, V>(name, context), context.metrics(), new SystemTime());
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V> {

        private final String topic;
        private final int partition;
        private final Set<K> dirty;
        private final int maxDirty;
        private final NavigableMap<K, V> map;
        private final KStreamContext context;

        @SuppressWarnings("unchecked")
        public MemoryStore(String name, KStreamContext context) {
            this.topic = name;
            this.partition = context.id();
            this.map = new TreeMap<K, V>();
            this.dirty = new HashSet<K>();
            this.maxDirty = 100;
            this.context = context;

            this.context.register(this);
        }

        @Override
        public String name() {
            return this.topic;
        }

        @Override
        public boolean persistent() { return false; }

        @Override
        public V get(K key) {
            return this.map.get(key);
        }

        @Override
        public void put(K key, V value) {
            this.map.put(key, value);
            if(context.recordCollector() != null) {
                this.dirty.add(key);
                if (this.dirty.size() > this.maxDirty)
                    flush();
            }
        }

        @Override
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public void delete(K key) {
            put(key, null);
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryStoreIterator<K, V>(this.map.subMap(from, true, to, false).entrySet().iterator());
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryStoreIterator<K, V>(this.map.entrySet().iterator());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void flush() {
            RecordCollector collector = context.recordCollector();
            Serializer<K> keySerializer = (Serializer<K>) context.keySerializer();
            Serializer<V> valueSerializer = (Serializer<V>) context.valueSerializer();

            if(collector != null) {
                for (K k : this.dirty) {
                    V v = this.map.get(k);
                    collector.send(new ProducerRecord<>(this.topic, this.partition, k, v), keySerializer, valueSerializer);
                }
                this.dirty.clear();
            }
        }

        @Override
        public void restore() {
            final Deserializer<K> keyDeserializer = (Deserializer<K>) context.keySerializer();
            final Deserializer<V> valDeserializer = (Deserializer<V>) context.valueSerializer();

            context.restore(this, new RestoreFunc () {
                @Override
                public void apply(byte[] key, byte[] value) {
                    map.put(keyDeserializer.deserialize(topic, key),
                        valDeserializer.deserialize(topic, value));
                }

                @Override
                public void load() {
                    // this should not happen since it is in-memory, hence no state to load from disk
                    throw new IllegalStateException("This should not happen");
                }
            });
        }

        @Override
        public void close() {
            flush();
        }

        private static class MemoryStoreIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<Map.Entry<K, V>> iter;

            public MemoryStoreIterator(Iterator<Map.Entry<K, V>> iter) {
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                Map.Entry<K, V> entry = iter.next();
                return new Entry<>(entry.getKey(), entry.getValue());
            }

            @Override
            public void remove() {
                iter.remove();
            }

            @Override
            public void close() {}

        }
    }

}
