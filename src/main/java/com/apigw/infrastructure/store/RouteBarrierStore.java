package com.apigw.infrastructure.store;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 路由 revision 的集群两阶段切换屏障。所有关键状态推进都走 Lua，避免多台网关并发协调时互相覆盖。
 *
 * <h3>生命周期与回收</h3>
 * 屏障是<b>协调运行时状态</b>，不是留档：barrier/ready/checksum/fenced 在 PREPARING 建立时统一带 TTL，
 * leader 锁本来就有短 TTL。协调者在新 revision 激活后，再把「已不可能再激活的更老 revision」
 * 的整套屏障 key 主动删掉（{@link #pruneSettledBefore}），只留仍在保留窗口内、可能被查询/追赶
 * 的最近若干个。双保险下，删除/失败的 revision 都不会让 Redis 越用越胀。
 */
@Component
public class RouteBarrierStore {

    /** 屏障 key 的兜底 TTL：正常由协调者主动回收；进程全挂、没人跑回收时也不会永久残留。 */
    public static final Duration BARRIER_TTL = Duration.ofDays(7);

    private static final RedisScript<Long> CREATE_PREPARING_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local readyKey = KEYS[2]
            local checksumKey = KEYS[3]
            local ttlSeconds = tonumber(ARGV[4])
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
            -- 协调运行时状态统一带 TTL：没人来回收时也会自己过期，不越积越多
            redis.call('EXPIRE', barrierKey, ttlSeconds)
            redis.call('EXPIRE', readyKey, ttlSeconds)
            redis.call('EXPIRE', checksumKey, ttlSeconds)
            return 1
            """, Long.class);

    private static final RedisScript<Long> MARK_READY_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local readyKey = KEYS[2]
            local checksumKey = KEYS[3]
            local ttlSeconds = tonumber(ARGV[4])
            if redis.call('HGET', barrierKey, 'state') ~= 'PREPARING' then return 0 end
            redis.call('HSET', readyKey, ARGV[1], ARGV[2])
            redis.call('HSET', checksumKey, ARGV[1], ARGV[3])
            redis.call('EXPIRE', readyKey, ttlSeconds)
            redis.call('EXPIRE', checksumKey, ttlSeconds)
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
            local ttlSeconds = tonumber(ARGV[6])
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
            redis.call('EXPIRE', fenceKey, ttlSeconds)
            redis.call('PUBLISH', channel, 'fence:' .. ARGV[5])
            return 1
            """, Long.class);

    private static final RedisScript<Long> MARK_FENCED_SCRIPT = new DefaultRedisScript<>(
            """
            local barrierKey = KEYS[1]
            local fenceKey = KEYS[2]
            local ttlSeconds = tonumber(ARGV[3])
            if redis.call('HGET', barrierKey, 'state') ~= 'FENCING' then return 0 end
            redis.call('HSET', fenceKey, ARGV[1], ARGV[2])
            redis.call('EXPIRE', fenceKey, ttlSeconds)
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
                List.of(Long.toString(revision), checksum, now,
                        Long.toString(BARRIER_TTL.toSeconds()))).next().map(v -> v == 1L);
    }

    public Mono<Boolean> markReady(long revision, String instanceJson, String checksum) {
        String instanceId = instanceId(instanceJson);
        return redis.execute(MARK_READY_SCRIPT,
                List.of(barrierKey(revision), readyKey(revision), checksumKey(revision)),
                List.of(instanceId, instanceJson, checksum,
                        Long.toString(BARRIER_TTL.toSeconds()))).next().map(v -> v == 1L);
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
                        Long.toString(revision),
                        Long.toString(BARRIER_TTL.toSeconds()))).next().map(v -> v == 1L);
    }

    public Mono<Boolean> markFenced(long revision, String instanceJson) {
        String instanceId = instanceId(instanceJson);
        return redis.execute(MARK_FENCED_SCRIPT,
                List.of(barrierKey(revision), fencedKey(revision)),
                List.of(instanceId, instanceJson,
                        Long.toString(BARRIER_TTL.toSeconds()))).next().map(v -> v == 1L);
    }

    public Mono<Boolean> activate(long revision, String instanceId, int expectedInstances,
                                  String checksum, String now) {
        return redis.execute(ACTIVATE_SCRIPT,
                List.of(barrierKey(revision), fencedKey(revision), checksumKey(revision),
                        RouteRevisionStore.ACTIVE_KEY, leaderKey(revision), RouteRevisionStore.CHANNEL),
                List.of(instanceId, Integer.toString(expectedInstances), checksum,
                        Long.toString(revision), now)).next().map(v -> v == 1L);
    }

    public Mono<Boolean> abort(long revision, String instanceIdOrStar, String reason, String now) {
        return redis.execute(ABORT_SCRIPT,
                List.of(barrierKey(revision), leaderKey(revision), RouteRevisionStore.CHANNEL),
                List.of(instanceIdOrStar, reason, now, Long.toString(revision))).next().map(v -> v == 1L);
    }

    /**
     * 回收「已经尘埃落定、不可能再被激活」的老 revision 的整套屏障 key
     * （barrier / ready / checksum / fenced / leader）。
     *
     * <p>单调性保证安全：revision 只增不减，一旦更高 revision 已经 active 或进入推进，
     * 任何 {@code < activeRevision} 的屏障都不会再被任何实例使用——它们既不服务流量
     * （流量只认 active 快照），也不再参与协调。删掉它们只回收运行时状态；
     * 不可变快照（audit/追赶用）在另一个 key 上，由 revision store 按保留窗口管理，不受影响。
     *
     * <p>仍在最近保留集合 {@code keep} 里的 revision 不动，给状态查询和短暂追赶留窗口。
     *
     * @param activeRevision 已经激活的 revision，严格小于它的才是回收候选
     * @param keep           仍在快照保留窗口里的 revision（这些即便更老也先不删）
     * @return 实际删掉的 key 数量
     */
    public Mono<Long> pruneSettledBefore(long activeRevision, java.util.Set<Long> keep) {
        return scanBarrierRevisions()
                .filter(rev -> rev < activeRevision && (keep == null || !keep.contains(rev)))
                .flatMap(rev -> Flux.just(
                                barrierKey(rev), readyKey(rev), checksumKey(rev),
                                fencedKey(rev), leaderKey(rev))
                        .collectList()
                        .flatMap(keys -> redis.delete(Flux.fromIterable(keys))))
                .reduce(0L, Long::sum);
    }

    /** 扫描现存屏障 key，解出各自的 revision。 */
    private Flux<Long> scanBarrierRevisions() {
        return redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match(RouteRevisionStore.BARRIER_PREFIX + "*").count(200).build())
                // 只要屏障主 key（barrier:{rev}），不收 :ready/:checksum/:fenced/:leader 后缀，
                // 后缀在删除时按主 revision 成套删
                .filter(key -> {
                    String k = key.toString();
                    String suffix = k.substring(RouteRevisionStore.BARRIER_PREFIX.length());
                    return !suffix.isBlank() && suffix.chars().allMatch(Character::isDigit);
                })
                .map(key -> Long.parseLong(
                        key.toString().substring(RouteRevisionStore.BARRIER_PREFIX.length())));
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
