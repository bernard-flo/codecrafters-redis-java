import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ExpiringMap {

    private final ConcurrentMap<String, Entry> map = new ConcurrentHashMap<>();

    public void put(String key, String value, /*@Nullable*/ Long expiryMs) {
        Entry entry = new Entry();
        entry.value = value;
        if (expiryMs != null) {
            entry.expiringTimeMs = Optional.of(System.currentTimeMillis() + expiryMs);
        } else {
            entry.expiringTimeMs = Optional.empty();
        }
        map.put(key, entry);
    }

    public Optional<String> get(String key) {
        Entry entry = map.get(key);
        if (entry == null) {
            return Optional.empty();
        }

        boolean expired = entry.expiringTimeMs.filter(x -> x <= System.currentTimeMillis()).isPresent();
        if (expired) {
            map.remove(key);
            return Optional.empty();
        } else {
            return Optional.of(entry.value);
        }
    }

    private static class Entry {
        String value;
        Optional<Long> expiringTimeMs;
    }

}
