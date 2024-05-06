package app_kvServer;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.HashMap;

class Cache {

    class CacheEntry {

        String key, val;
        int freq = 1;

        CacheEntry(String k, String v) {key = k; val = v;}
    }

    private int cacheSize;
    private Map<String, CacheEntry> cache;
    private PriorityQueue<CacheEntry> minHeap;

    public Cache(int size) {
        cacheSize = size;
        cache = new HashMap<>();
        minHeap = new PriorityQueue<>((a, b) -> a.freq - b.freq);
    }

    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    public String read(String key) {
        CacheEntry cacheEntry = cache.get(key);
        minHeap.remove(cacheEntry);
        cacheEntry.freq += 1;
        minHeap.offer(cacheEntry);
        return cacheEntry.val;
    }

    public void write(String key, String val) {
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            if (cache.size() == cacheSize) {
                CacheEntry lfuEntry = minHeap.poll();
                cache.remove(lfuEntry.key);
            }
            cacheEntry = new CacheEntry(key, val);
            cache.put(key, cacheEntry);
        } else {
            minHeap.remove(cacheEntry);
            cacheEntry.freq += 1;
        }
        minHeap.offer(cacheEntry);
    }

    public void delete(String key) {
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null)
            return;
        cache.remove(key);
        minHeap.remove(cacheEntry);
    }
}