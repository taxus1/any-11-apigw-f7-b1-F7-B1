package com.apigw.infrastructure.store;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * 路由 revision 的集群两阶段切换屏障。所有关键状态推进都走 Lua，避免多台网关并发协调时互相覆盖。
 *
 * <p>每个 revision 的屏障 key 都带 TTL：屏障是一次性协调过程状态，协调完（ACTIVE/ABORTED）
 * 就只剩排障价值。若不显式过期，每改一次路由就永久攒下一组 barrier/ready/fenced/checksum key，
 * 存储只增不减。TTL 与不可变快照对齐（7 天），排障窗口内可查，到期由 Redis 自动回收；
 * leader 锁本来就是短 TTL。
 */
@Component
public class RouteBarrierStore {

    /** 屏障及其 ready/fenced/checksum 附属 key 的保留时间：与快照 TTL 对齐，过期自动回收。 */
    private static final String BARRIER_TTL_SECONDS = "604800";

    private static final RedisScript<Long> CREATE_PREPARING_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local readyKey = KEYS[2]
            local checksumKey = KEYS[3]
            local ttl = tonumber(ARGV[4])
            local state = redis.call('HGET', barrierKey, 'state')
            if state ~= false and state ~= 'ABORTED' then return 1 end
            redis.call('DEL', barrierKey)
            redis.call('DEL', readyKey)
            redis.call('DEL', checksumKey)
            redis.call('HSET', barrierKey,
              'state', 'PREPARING',
              'revision', ARGV[1],
              'checksum', ARGV[2],
              'createdAt', ARGV[3],
              'updatedAt', ARGV[3])
            redis.call('DEL', readyKey)
            redis.call('DEL', checksumKey)
            redis.call('EXPIRE', barrierKey, ttl)
            return 1
            """, Long.class);

    private static final RedisScript<Long> MARK_READY_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local readyKey = KEYS[2]
            local checksumKey = KEYS[3]
            local ttl = tonumber(ARGV[4])
            if redis.call('HGET', barrierKey, 'state') ~= 'PREPARING' then return 0 end
            redis.call('HSET', readyKey, ARGV[1], ARGV[2])
            redis.call('HSET', checksumKey, ARGV[1], ARGV[3])
            redis.call('EXPIRE', readyKey, ttl)
            redis.call('EXPIRE', checksumKey, ttl)
            return 1
            """, Long.class);

    private static final RedisScript<Long> ACQUIRE_LEADER_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local lockKey = KEYS[2]
            local state = redis.call('HGET', barrierKey, 'state')
            if state ~= 'PREPARING' and state ~= 'FENCING' then return 0 end
            local owner = redis.call('GET', lockKey)
            if owner ~= false and owner ~= ARGV[1] then return 0 end
            redis.call('SET', lockKey, ARGV[1], 'PX', ARGV[2])
            redis.call('PEXPIRE', lockKey, ARGV[2])
            return 1
            """, Long.class);

    private static final RedisScript<Long> BEGIN_FENCING_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local readyKey = KEYS[2]
            local checksumKey = KEYS[3]
            local fenceKey = KEYS[4]
            local lockKey = KEYS[5]
            local channel = KEYS[6]
            if redis.call('GET', lockKey) ~= ARGV[1] then return 0 end
            if redis.call('HGET', barrierKey, 'state') ~= 'PREPARING' then return 0 end
            local expected = tonumber(ARGV[2])
            local expectedChecksum = ARGV[3]
            local ready = redis.call('HGETALL', readyKey)
            local checksums = redis.call('HGETALL', checksumKey)
            if #ready / 2 < expected then return 0 end
            for i = 2, #checksums, 2 do
                if checksums[i] ~= expectedChecksum then return 0 end
            end
            redis.call('DEL', fenceKey)
            redis.call('HSET', barrierKey, 'state', 'FENCING', 'fenceAt', ARGV[4], 'updatedAt', ARGV[4])
            redis.call('EXPIRE', barrierKey, tonumber(ARGV[6]))
            redis.call('PUBLISH', channel, 'fence:' .. ARGV[5])
            return 1
            """, Long.class);

    private static final RedisScript<Long> MARK_FENCED_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local fenceKey = KEYS[2]
            local ttl = tonumber(ARGV[3])
            if redis.call('HGET', barrierKey, 'state') ~= 'FENCING' then return 0 end
            redis.call('HSET', fenceKey, ARGV[1], ARGV[2])
            redis.call('EXPIRE', fenceKey, ttl)
            redis.call('EXPIRE', barrierKey, ttl)
            return 1
            """, Long.class);

    private static final RedisScript<Long> ACTIVATE_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local fenceKey = KEYS[2]
            local checksumKey = KEYS[3]
            local activeKey = KEYS[4]
            local lockKey = KEYS[5]
            local channel = KEYS[6]
            if redis.call('GET', lockKey) ~= ARGV[1] then return 0 end
            if redis.call('HGET', barrierKey, 'state') ~= 'FENCING' then return 0 end
            local expected = tonumber(ARGV[2])
            local expectedChecksum = ARGV[3]
            local fenced = redis.call('HGETALL', fenceKey)
            local checksums = redis.call('HGETALL', checksumKey)
            if #fenced / 2 < expected then return 0 end
            for i = 2, #checksums, 2 do
              if checksums[i] ~= expectedChecksum then return 0 end
            end
            local now = ARGV[5]
            redis.call('HSET', activeKey, 'revision', ARGV[4], 'checksum', expectedChecksum, 'activatedAt', now)
            redis.call('HSET', barrierKey, 'state', 'ACTIVE', 'updatedAt', now)
            redis.call('EXPIRE', barrierKey, tonumber(ARGV[6]))
            redis.call('DEL', lockKey)
            redis.call('PUBLISH', channel, 'activate:' .. ARGV[4])
            return 1
            """, Long.class);

    private static final RedisScript<Long> ABORT_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local lockKey = KEYS[2]
            local channel = KEYS[3]
            local state = redis.call('HGET', barrierKey, 'state')
            if state == false or state == 'ACTIVE' then return 0 end
            if ARGV[1] ~= '*' and redis.call('GET', lockKey) ~= ARGV[1] then return 0 end
            local now = ARGV[3]
            redis.call('HSET', barrierKey, 'state', 'ABORTED', 'error', ARGV[2], 'updatedAt', now)
            redis.call('EXPIRE', barrierKey, tonumber(ARGV[5]))
            redis.call('DEL', lockKey)
            redis.call('PUBLISH', channel, 'abort:' .. ARGV[4])
            return 1
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RouteBarrierStore(ReactiveStringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public static String barrierKey(long revision) {
        return RouteRevisionStore.BARRIER_PREFIX + revision;
    }

    public static String readyKey(long revision) {
        return barrierKey(revision) + ":ready";
    }

    public static String checksumKey(long revision) {
        return barrierKey(revision) + ":checksum";
    }

    public static String fencedKey(long revision) {
        return barrierKey(revision) + ":fenced";
    }

    public static String leaderKey(long revision) {
        return barrierKey(revision) + ":leader";
    }

    public Mono<Map<String, String>> barrier(long revision) {
        return readHash(barrierKey(revision));
    }

    public Mono<Map<String, String>> ready(long revision) {
        return readHash(readyKey(revision));
    }

    public Mono<Map<String, String>> fenced(long revision) {
        return readHash(fencedKey(revision));
    }

    public Mono<Boolean> createPreparing(long revision, String checksum, String now) {
        return redis.execute(CREATE_PREPARING_SCRIPT,
                List.of(barrierKey(revision), readyKey(revision), checksumKey(revision)),
                List.of(Long.toString(revision), checksum, now, BARRIER_TTL_SECONDS)).next()
                .map(v -> v == 1L);
    }

    public Mono<Boolean> markReady(long revision, String instanceJson, String checksum) {
        String instanceId = instanceId(instanceJson);
        return redis.execute(MARK_READY_SCRIPT,
                List.of(barrierKey(revision), readyKey(revision), checksumKey(revision)),
                List.of(instanceId, instanceJson, checksum, BARRIER_TTL_SECONDS)).next()
                .map(v -> v == 1L);
    }

    public Mono<Boolean> acquireLeader(long revision, String instanceId, long ttlMs) {
        return redis.execute(ACQUIRE_LEADER_SCRIPT,
                List.of(barrierKey(revision), leaderKey(revision)),
                List.of(instanceId, Long.toString(ttlMs))).next().map(v -> v == 1L);
    }

    public Mono<Boolean> beginFencing(long revision, String instanceId, int expectedInstances,
                                      String checksum, String now) {
        return redis.execute(BEGIN_FENCING_SCRIPT,
                List.of(barrierKey(revision), readyKey(revision), checksumKey(revision),
                        fencedKey(revision), leaderKey(revision), RouteRevisionStore.CHANNEL),
                List.of(instanceId, Integer.toString(expectedInstances), checksum, now,
                        Long.toString(revision), BARRIER_TTL_SECONDS)).next().map(v -> v == 1L);
    }

    public Mono<Boolean> markFenced(long revision, String instanceJson) {
        String instanceId = instanceId(instanceJson);
        return redis.execute(MARK_FENCED_SCRIPT,
                List.of(barrierKey(revision), fencedKey(revision)),
                List.of(instanceId, instanceJson, BARRIER_TTL_SECONDS)).next().map(v -> v == 1L);
    }

    public Mono<Boolean> activate(long revision, String instanceId, int expectedInstances,
                                  String checksum, String now) {
        return redis.execute(ACTIVATE_SCRIPT,
                List.of(barrierKey(revision), fencedKey(revision), checksumKey(revision),
                        RouteRevisionStore.ACTIVE_KEY, leaderKey(revision), RouteRevisionStore.CHANNEL),
                List.of(instanceId, Integer.toString(expectedInstances), checksum,
                        Long.toString(revision), now, BARRIER_TTL_SECONDS)).next().map(v -> v == 1L);
    }

    public Mono<Boolean> abort(long revision, String instanceIdOrStar, String reason, String now) {
        return redis.execute(ABORT_SCRIPT,
                List.of(barrierKey(revision), leaderKey(revision), RouteRevisionStore.CHANNEL),
                List.of(instanceIdOrStar, reason, now, Long.toString(revision),
                        BARRIER_TTL_SECONDS)).next().map(v -> v == 1L);
    }

    private String instanceId(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            return node.path("instanceId").asText();
        } catch (Exception e) {
            throw new IllegalArgumentException("实例状态 JSON 缺少 instanceId", e);
        }
    }

    private Mono<Map<String, String>> readHash(String key) {
        return redis.opsForHash().entries(key)
                .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString());
    }
}
