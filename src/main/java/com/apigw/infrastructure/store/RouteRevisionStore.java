package com.apigw.infrastructure.store;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 路由全局 revision 与不可变快照仓库。
 *
 * <p>原 {@code apigw:routes} Hash 结构不变；这里额外维护 revision、snapshot 和 active 指针。
 * 路由写入、revision 递增、全量快照复制、通知在同一个 Lua 脚本中完成，Redis 单线程执行脚本，
 * 因而不会读到跨两次提交拼出来的全量路由集合。
 */
@Component
public class RouteRevisionStore {

    public static final String ROUTES_KEY = RouteStore.ROUTES_KEY;
    public static final String REVISION_KEY = "apigw:route:revision";
    public static final String ACTIVE_KEY = "apigw:route:active";
    public static final String SNAPSHOT_INDEX_KEY = "apigw:route:snapshot:index";
    public static final String CHANNEL = "apigw:route:channel";
    public static final String INSTANCES_KEY = "apigw:route:instances";
    public static final String BARRIER_PREFIX = "apigw:route:barrier:";
    /** 规则索引 key：提交脚本删除路由时一并清掉同一编号的投影 field。 */
    public static final String RULES_KEY = RouteRuleIndex.RULES_KEY;

    private static final Duration SNAPSHOT_TTL = Duration.ofDays(7);

    private static final String SNAPSHOT_PREFIX = "apigw:route:snapshot:";

    /**
     * 返回值：{0=成功, 1=编号占用, 2=路由不存在, 3=版本冲突}，后续元素为 revision / routeVersion。
     *
     * <p>DELETE 与规则索引 {@code apigw:route:rules} 的 HDEL 在同一个脚本里完成：
     * 权威 field 与派生投影必须同生共死，不能让「同编号立刻重建」落在两步之间，
     * 把旧条件/动作残留在索引里再被新路由捡走。
     */
    private static final RedisScript<List> COMMIT_SCRIPT = new DefaultRedisScript<>(
            """
            local routesKey = KEYS[1]
            local revisionKey = KEYS[2]
            local indexKey = KEYS[3]
            local channel = KEYS[4]
            local rulesKey = KEYS[5]
            local op = ARGV[1]
            local routeNo = ARGV[2]
            local payload = ARGV[3]
            local expectVersion = ARGV[4]
            local retention = tonumber(ARGV[5])
            local ttlSeconds = tonumber(ARGV[6])

            local function currentVersionOf(json)
              return tonumber(string.match(json, '"version"%s*:%s*(%d+)') or '0')
            end
            local existing = redis.call('HGET', routesKey, routeNo)
            local nextRouteVersion = 0

            if op == 'CREATE' then
              if existing ~= false then
                return {1, '路由编号已被占用（停用的路由也占号）：' .. routeNo, 0, 0, ''}
              end
              -- Java 侧 RouteStore.create 已把 version 钉为 0；Lua 不回编 JSON，避免空数组被编成 {}。
              redis.call('HSET', routesKey, routeNo, payload)
            elseif op == 'UPDATE' then
              if existing == false then
                return {2, '路由不存在：' .. routeNo, 0, 0, ''}
              end
              local currentVersion = currentVersionOf(existing)
              if tostring(currentVersion) ~= tostring(expectVersion) then
                return {3, '你这份配置已经旧了（当前版本 ' .. currentVersion .. '，你手上是 ' .. expectVersion .. '），请重新拉取后再提交', currentVersion, 0, ''}
              end
              -- Java 侧已经按现有 id/version 重写好完整 JSON；Lua 不使用 cjson 回编，避免空数组被编成 {}。
              nextRouteVersion = currentVersion + 1
              redis.call('HSET', routesKey, routeNo, payload)
            elseif op == 'DELETE' then
              if existing == false then
                return {2, '路由不存在，删除未执行：' .. routeNo, 0, 0, ''}
              end
              if expectVersion ~= '' then
                local currentVersion = currentVersionOf(existing)
                if tostring(currentVersion) ~= tostring(expectVersion) then
                  return {3, '你这份配置已经旧了（当前版本 ' .. currentVersion .. '，你手上是 ' .. expectVersion .. '），请重新拉取后再提交', currentVersion, 0, ''}
                end
              end
              redis.call('HDEL', routesKey, routeNo)
              -- 派生投影（规则索引）随权威 field 同一次提交清掉，不留无主记录，也不污染同编号重建
              redis.call('HDEL', rulesKey, routeNo)
            else
              return {9, '未知路由提交类型：' .. op, 0, 0, ''}
            end

            local revision = redis.call('INCR', revisionKey)
            local snapshotKey = 'apigw:route:snapshot:' .. revision
            redis.call('DEL', snapshotKey)
            local all = redis.call('HGETALL', routesKey)
            for i = 1, #all, 2 do
              redis.call('HSET', snapshotKey, all[i], all[i + 1])
            end
            redis.call('EXPIRE', snapshotKey, ttlSeconds)
            redis.call('ZADD', indexKey, revision, tostring(revision))
            redis.call('EXPIRE', indexKey, ttlSeconds)
            local removeCount = redis.call('ZCARD', indexKey) - retention
            if removeCount > 0 then
              local old = redis.call('ZRANGE', indexKey, 0, removeCount - 1)
              for i = 1, #old do
                redis.call('DEL', 'apigw:route:snapshot:' .. old[i])
              end
              redis.call('ZREMRANGEBYRANK', indexKey, 0, removeCount - 1)
            end
            redis.call('PUBLISH', channel, 'committed:' .. revision)
            return {0, 'ok', revision, nextRouteVersion}
            """, List.class);

    private static final RedisScript<Long> BOOTSTRAP_SCRIPT = new DefaultRedisScript<>(
            """
            local revisionKey = KEYS[1]
            local activeKey = KEYS[2]
            local indexKey = KEYS[3]
            local routesKey = KEYS[4]
            if redis.call('HGET', activeKey, 'revision') ~= false then
              return tonumber(redis.call('HGET', activeKey, 'revision'))
            end
            local revision = tonumber(redis.call('GET', revisionKey) or '0')
            if revision == 0 and redis.call('HLEN', routesKey) > 0 then
              revision = redis.call('INCR', revisionKey)
              local snapshotKey = 'apigw:route:snapshot:' .. revision
              local all = redis.call('HGETALL', routesKey)
              for i = 1, #all, 2 do
                redis.call('HSET', snapshotKey, all[i], all[i + 1])
              end
              redis.call('EXPIRE', snapshotKey, 604800)
              redis.call('ZADD', indexKey, revision, tostring(revision))
            end
            if revision == 0 then
              revision = redis.call('INCR', revisionKey)
            end
            redis.call('HSET', activeKey, 'revision', revision)
            return revision
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;

    public RouteRevisionStore(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    public Mono<CommitResult> commitCreate(String routeNo, String json) {
        return executeCommit("CREATE", routeNo, json, "");
    }

    public Mono<CommitResult> commitUpdate(String routeNo, String json, int expectVersion) {
        return executeCommit("UPDATE", routeNo, json, Integer.toString(expectVersion));
    }

    public Mono<CommitResult> commitDelete(String routeNo, Integer expectVersion) {
        return executeCommit("DELETE", routeNo, "", expectVersion == null ? "" : expectVersion.toString());
    }

    public Mono<Long> bootstrapActiveRevision() {
        return redis.execute(BOOTSTRAP_SCRIPT,
                List.of(REVISION_KEY, ACTIVE_KEY, SNAPSHOT_INDEX_KEY, ROUTES_KEY)).next();
    }

    public Mono<Long> latestRevision() {
        return redis.opsForValue().get(REVISION_KEY)
                .map(Long::valueOf)
                .defaultIfEmpty(0L);
    }

    public Mono<Map<String, String>> loadSnapshot(long revision) {
        return redis.opsForHash().entries(SNAPSHOT_PREFIX + revision)
                .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString());
    }

    public Mono<Map<String, String>> active() {
        return redis.opsForHash().entries(ACTIVE_KEY)
                .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString());
    }

    public Mono<Boolean> publish(String message) {
        return redis.convertAndSend(CHANNEL, message).map(count -> true).defaultIfEmpty(false);
    }

    @SuppressWarnings("unchecked")
    private Mono<CommitResult> executeCommit(String op, String routeNo, String json, String expectVersion) {
        return redis.execute(COMMIT_SCRIPT,
                        List.of(ROUTES_KEY, REVISION_KEY, SNAPSHOT_INDEX_KEY, CHANNEL, RULES_KEY),
                        List.of(op, routeNo, json, expectVersion, "20", Long.toString(SNAPSHOT_TTL.getSeconds())))
                .next()
                .map(raw -> {
                    List<Object> values = (List<Object>) raw;
                    int code = ((Number) values.get(0)).intValue();
                    String message = String.valueOf(values.get(1));
                    long revision = ((Number) values.get(2)).longValue();
                    int routeVersion = ((Number) values.get(3)).intValue();
                    return new CommitResult(code, message, revision, routeVersion, null);
                });
    }

    public record CommitResult(int code, String message, long revision, int routeVersion, String checksum) {
        public boolean success() {
            return code == 0;
        }
    }
}
