package com.apigw.proxy.route;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.infrastructure.store.RouteBarrierStore;
import com.apigw.infrastructure.store.RouteRevisionStore;
import com.apigw.infrastructure.store.RouteStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 路由 revision 的多实例协调器。
 *
 * <p>该组件把“提交好的快照”推进成“全集群允许接流量的活动版本”：所有实例先 READY，
 * 再统一进入短栅栏并原子切换；任何一个预期实例未跟上，就保持旧活动版本继续转发。
 */
@Slf4j
@Service
public class RouteCoordinator {

    private final RouteRevisionStore revisionStore;
    private final RouteBarrierStore barrierStore;
    private final RouteStore routeStore;
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final RouteCoordinationProperties properties;
    private final ActivationGate gate;
    private final RouteRuntimeStateReaper runtimeStateReaper;

    private volatile RouteSnapshot current;
    private volatile Long activeRevision;
    private volatile long latestRevision;
    private volatile String state = "STARTING";
    private volatile String lastError;
    private volatile Instant lastLoadAt;
    private volatile Instant heartbeatAt;

    private final Map<Long, RouteSnapshot> staged = new ConcurrentHashMap<>();
    private final AtomicLong tickGeneration = new AtomicLong();

    /** 本机暂存候选快照的上界，防止协调反复中断时旧候选在进程里只增不减。 */
    private static final int MAX_STAGED_SNAPSHOTS = 10;

    @org.springframework.beans.factory.annotation.Autowired
    public RouteCoordinator(RouteRevisionStore revisionStore,
                            RouteBarrierStore barrierStore,
                            RouteStore routeStore,
                            ReactiveStringRedisTemplate redis,
                            ObjectMapper objectMapper,
                            RouteCoordinationProperties properties,
                            RouteRuntimeStateReaper runtimeStateReaper) {
        this.revisionStore = revisionStore;
        this.barrierStore = barrierStore;
        this.routeStore = routeStore;
        this.redis = redis;
        this.objectMapper = objectMapper.findAndRegisterModules();
        this.properties = properties;
        this.runtimeStateReaper = runtimeStateReaper;
        this.gate = new ActivationGate(properties.gateWaitTimeout());
    }

    /** 测试/兼容装配：不带运行时状态回收器时，跳过本机派生状态对账。 */
    public RouteCoordinator(RouteRevisionStore revisionStore,
                            RouteBarrierStore barrierStore,
                            RouteStore routeStore,
                            ReactiveStringRedisTemplate redis,
                            ObjectMapper objectMapper,
                            RouteCoordinationProperties properties) {
        this(revisionStore, barrierStore, routeStore, redis, objectMapper, properties, null);
    }

    @PostConstruct
    void start() {
        if (!properties.enabled()) {
            return;
        }
        redis.listenToChannel(RouteRevisionStore.CHANNEL)
                .doOnNext(message -> Schedulers.parallel().schedule(() ->
                        onChannelMessage(message.getMessage())))
                .onErrorContinue((err, value) -> log.debug("路由协调订阅消息失败：{}", err.toString()))
                .subscribe();
        Schedulers.parallel().schedulePeriodically(this::safeTick,
                properties.pollInterval().toMillis(),
                properties.pollInterval().toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
        Schedulers.parallel().schedulePeriodically(this::safeHeartbeat,
                0,
                properties.heartbeatInterval().toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public boolean isEnabled() {
        return properties.enabled();
    }

    public Mono<RouteSnapshot> snapshot() {
        if (!properties.enabled()) {
            return Mono.empty();
        }
        RouteSnapshot snapshot = current;
        if (snapshot == null) {
            return Mono.error(new BizException(503, "路由配置尚未加载完成"));
        }
        return gate.awaitOpen().thenReturn(snapshot);
    }

    public Mono<CoordinationStatus> refresh() {
        if (!properties.enabled()) {
            return Mono.error(new BizException("路由协调未启用"));
        }
        return Mono.fromRunnable(this::safeTick).then(status());
    }

    public Mono<CoordinationStatus> status() {
        return revisionStore.latestRevision()
                .flatMap(latest -> revisionStore.active().flatMap(active ->
                        barrierStore.barrier(latest).flatMap(barrier ->
                                instances(latest).map(instances -> new CoordinationStatus(
                                        parseLong(active.get("revision")),
                                        latest,
                                        barrier.getOrDefault("state", latest == parseLong(active.get("revision"))
                                                ? "ACTIVE" : "IDLE"),
                                        barrier.getOrDefault("error", null),
                                        properties.expectedInstances(),
                                        current == null ? null : current.revision(),
                                        lastLoadAt,
                                        heartbeatAt,
                                        instances)))))
                .onErrorResume(err -> Mono.just(new CoordinationStatus(
                        current == null ? null : current.revision(),
                        latestRevision,
                        state,
                        err.toString(),
                        properties.expectedInstances(),
                        current == null ? null : current.revision(),
                        lastLoadAt,
                        heartbeatAt,
                        List.of(localInstance("UNHEALTHY", err.toString())))));
    }

    public Mono<Boolean> forceActivate(long revision, String reason) {
        if (reason == null || reason.isBlank()) {
            return Mono.error(new BizException("强制激活必须填写 reason，便于审计"));
        }
        return ensureSnapshot(revision)
                .flatMap(snapshot -> {
                    Mono<Boolean> prepared = writeInstance(revision, "READY", null)
                            .then(barrierStore.createPreparing(revision, snapshot.checksum(), now()))
                            .then(barrierStore.markReady(revision, instanceJson(revision, "READY", null), snapshot.checksum()))
                            .then(barrierStore.acquireLeader(revision, properties.instanceId(), 30_000L));
                    Mono<Boolean> activated = prepared
                            .flatMap(leader -> Boolean.TRUE.equals(leader)
                                    ? barrierStore.beginFencing(revision, properties.instanceId(),
                                            properties.expectedInstances(), snapshot.checksum(), now())
                                    : Mono.just(false))
                            .flatMap(fencing -> Boolean.TRUE.equals(fencing)
                                    ? completeFencing(revision, snapshot, properties.expectedInstances())
                                    : Mono.just(false))
                            .map(Boolean.class::cast);
                    return activated.doOnSuccess(ok -> log.warn(
                            "手动强制激活路由 revision={} result={} reason={}", revision, ok, reason));
                });
    }

    public ActivationGate gate() {
        return gate;
    }

    public RouteSnapshot currentSnapshot() {
        return current;
    }

    private void safeTick() {
        long generation = tickGeneration.incrementAndGet();
        tick(generation).subscribe(
                v -> { },
                e -> {
                    lastError = e.toString();
                    log.warn("路由协调 tick 失败，继续使用旧快照", e);
                });
    }

    private Mono<Void> tick(long generation) {
        return revisionStore.bootstrapActiveRevision()
                .flatMap(activeRev -> revisionStore.latestRevision().flatMap(latest -> {
                    this.activeRevision = activeRev;
                    this.latestRevision = latest;
                    if (current == null || current.revision() != activeRev) {
                        return ensureSnapshot(activeRev).doOnNext(snapshot -> activateLocal(snapshot, "ACTIVE")).then();
                    }
                    if (latest > activeRev) {
                        return advance(latest, generation);
                    }
                    state = "ACTIVE";
                    lastError = null;
                    return writeHeartbeat();
                })).then();
    }

    private Mono<Void> advance(long revision, long generation) {
        return ensureSnapshot(revision)
                .flatMap(snapshot -> revisionStore.active()
                        .flatMap(activeHash -> {
                            long activeRevisionNo = parseLong(activeHash.get("revision"));
                            if (revision == activeRevisionNo || revision <= activeRevisionNo) {
                                return Mono.empty();
                            }
                            return barrierStore.barrier(revision).flatMap(barrier -> {
                                String barrierState = barrier.getOrDefault("state", "MISSING");
                                return switch (barrierState) {
                                    case "MISSING" -> createBarrier(revision, snapshot);
                                    case "PREPARING" -> preparing(revision, snapshot, barrier, generation);
                                    case "FENCING" -> fencing(revision, snapshot, barrier, generation);
                                    case "ACTIVE" -> ensureActive(revision, snapshot);
                                    case "ABORTED" -> retryAborted(revision, snapshot, barrier);
                                    default -> Mono.empty();
                                };
                            });
                        }).then());
    }

    private Mono<Void> createBarrier(long revision, RouteSnapshot snapshot) {
        return barrierStore.createPreparing(revision, snapshot.checksum(), now())
                .then(writeReady(revision, snapshot))
                .then(Mono.fromRunnable(() -> state = "PREPARING"));
    }

    private Mono<Void> preparing(long revision, RouteSnapshot snapshot, Map<String, String> barrier,
                                 long generation) {
        return writeReady(revision, snapshot).then(barrierStore.ready(revision).flatMap(ready ->
                barrierStore.acquireLeader(revision, properties.instanceId(),
                        properties.heartbeatTtl().toMillis() * 2).flatMap(leader -> {
                    if (ready.size() < properties.expectedInstances()) {
                        state = "WAITING_READY";
                        return abortIfTimedOut(revision, barrier, "createdAt",
                                properties.prepareTimeout(), "等待实例准备超时").thenReturn(false);
                    }
                    if (!leader) {
                        state = "WAITING_LEADER";
                        return Mono.just(false);
                    }
                    return barrierStore.beginFencing(revision, properties.instanceId(),
                            properties.expectedInstances(), snapshot.checksum(), now());
                })).flatMap(fencingStarted -> Boolean.TRUE.equals(fencingStarted)
                        ? publishLocalFence(revision, snapshot, generation)
                        : Mono.empty()).then());
    }

    private Mono<Void> fencing(long revision, RouteSnapshot snapshot, Map<String, String> barrier,
                               long generation) {
        return publishLocalFence(revision, snapshot, generation)
                .then(barrierStore.fenced(revision).flatMap(fenced ->
                        barrierStore.acquireLeader(revision, properties.instanceId(),
                                properties.heartbeatTtl().toMillis() * 2).flatMap(leader -> {
                            if (fenced.size() < properties.expectedInstances()) {
                                state = "WAITING_FENCED";
                                return abortIfTimedOut(revision, barrier, "fenceAt",
                                        properties.fenceTimeout(), "等待实例进入切换栅栏超时")
                                        .thenReturn(false);
                            }
                            if (!leader) {
                                return Mono.just(false);
                            }
                            return barrierStore.activate(revision, properties.instanceId(),
                                    properties.expectedInstances(), snapshot.checksum(), now());
                        })).flatMap(activated -> Boolean.TRUE.equals(activated)
                        ? Mono.fromRunnable(() -> activateLocal(snapshot, "ACTIVE"))
                        : Mono.empty()).then());
    }

    private Mono<Void> publishLocalFence(long revision, RouteSnapshot snapshot, long generation) {
        if (current != null && current.revision() == revision) {
            return Mono.empty();
        }
        if (!gate.armed()) {
            gate.arm();
        }
        return barrierStore.markFenced(revision, instanceJson(revision, "FENCED", null))
                .doOnNext(ok -> state = "FENCING")
                .then();
    }

    private Mono<Void> completeFencing(long revision, RouteSnapshot snapshot, int expected) {
        return Mono.defer(() -> {
            gate.arm();
            return barrierStore.markFenced(revision, instanceJson(revision, "FENCED", null))
                    .then(Mono.delay(Duration.ofMillis(100)))
                    .then(barrierStore.fenced(revision).flatMap(fenced ->
                            fenced.size() >= expected
                                    ? barrierStore.activate(revision, properties.instanceId(), expected,
                                            snapshot.checksum(), now())
                                    : Mono.just(false)))
                    .doFinally(sig -> gate.open())
                    .doOnNext(ok -> {
                        if (Boolean.TRUE.equals(ok)) {
                            activateLocal(snapshot, "ACTIVE");
                        }
                    }).then();
        });
    }

    private Mono<Void> ensureActive(long revision, RouteSnapshot snapshot) {
        return revisionStore.active().flatMap(active -> {
            long activeRev = parseLong(active.get("revision"));
            if (activeRev == revision && checksumMatches(snapshot, active.get("checksum"))) {
                activateLocal(snapshot, "ACTIVE");
            }
            return Mono.empty();
        });
    }

    private Mono<Void> retryAborted(long revision, RouteSnapshot snapshot, Map<String, String> barrier) {
        Instant updatedAt = parseInstant(barrier.get("updatedAt"));
        if (updatedAt != null && Instant.now().isBefore(updatedAt.plusSeconds(2))) {
            return Mono.empty();
        }
        lastError = barrier.get("error");
        return barrierStore.createPreparing(revision, snapshot.checksum(), now())
                .then(writeReady(revision, snapshot));
    }

    private Mono<Boolean> abortIfTimedOut(long revision, Map<String, String> barrier,
                                          String timeField, Duration timeout, String reason) {
        Instant since = parseInstant(barrier.get(timeField));
        if (since == null || Instant.now().isBefore(since.plus(timeout))) {
            return Mono.just(false);
        }
        return barrierStore.abort(revision, "*", reason, now());
    }

    private Mono<Void> writeReady(long revision, RouteSnapshot snapshot) {
        return writeInstance(revision, "READY", null)
                .then(barrierStore.markReady(
                        revision, instanceJson(revision, "READY", null), snapshot.checksum()).then());
    }

    private Mono<RouteSnapshot> ensureSnapshot(long revision) {
        RouteSnapshot existing = staged.get(revision);
        if (existing != null) {
            return Mono.just(existing);
        }
        if (current != null && current.revision() == revision) {
            return Mono.just(current);
        }
        return revisionStore.loadSnapshot(revision)
                .flatMap(raw -> Mono.fromCallable(() -> buildSnapshot(revision, raw)))
                .doOnNext(snapshot -> {
                    staged.put(revision, snapshot);
                    lastLoadAt = Instant.now();
                    lastError = null;
                });
    }

    private RouteSnapshot buildSnapshot(long revision, Map<String, String> raw) {
        if (raw.isEmpty()) {
            throw new BizException(503, "路由快照不存在或已过期：revision=" + revision);
        }
        TreeMap<String, String> sorted = new TreeMap<>(raw);
        String storedChecksum = sorted.remove("__checksum");
        if (sorted.isEmpty()) {
            storedChecksum = storedChecksum == null ? storedChecksum : storedChecksum;
        }
        List<GatewayRoute> routes = new ArrayList<>();
        for (String json : sorted.values()) {
            GatewayRoute route = routeStore.deserialize(json);
            if (Integer.valueOf(1).equals(route.getEnabled())
                    && route.getConditions() != null && !route.getConditions().isEmpty()) {
                routes.add(route);
            }
        }
        routes.sort(Comparator.comparing(GatewayRoute::getRouteNo));
        String checksum = storedChecksum != null ? storedChecksum : checksum(sorted);
        return new RouteSnapshot(revision, checksum, List.copyOf(routes), Instant.now(), Instant.now());
    }

    private boolean checksumMatches(RouteSnapshot snapshot, String expected) {
        return expected == null || Objects.equals(snapshot.checksum(), expected);
    }

    /**
     * 一份快照在本实例成为活动版本。这里是协调模式下唯一的「本机生效点」：
     * 切换栅栏开门、换 current 之外，还按新快照对账本机派生运行时状态——被删路由名下的
     * 熔断器/灰度计划在这一步收干净，所有实例都随各自的快照激活做同一件事。
     */
    private void activateLocal(RouteSnapshot snapshot, String newState) {
        gate.open();
        current = snapshot;
        activeRevision = snapshot.revision();
        latestRevision = Math.max(latestRevision, snapshot.revision());
        state = newState;
        lastError = null;
        staged.remove(snapshot.revision());
        pruneStaged(snapshot.revision());
        if (runtimeStateReaper != null) {
            runtimeStateReaper.reconcile(snapshot.routes());
        }
        safeHeartbeat();
    }

    /**
     * staged 只该短暂持有「准备中、尚未激活」的快照。协调反复中断（某台一直没 READY 导致旧版本
     * 卡在 PREPARING/FENCING）时，这里可能攒下一串再也不会激活的快照；以当前激活版本为界，
     * 只保留比它新的少量候选，旧的全部丢弃（Redis 里的不可变快照不受影响，需要时可重新装载）。
     */
    private void pruneStaged(long activeRev) {
        staged.keySet().removeIf(rev -> rev <= activeRev);
        if (staged.size() <= MAX_STAGED_SNAPSHOTS) {
            return;
        }
        staged.keySet().stream().sorted().limit(staged.size() - MAX_STAGED_SNAPSHOTS)
                .forEach(staged::remove);
    }

    private void onChannelMessage(String message) {
        try {
            if (message == null) {
                return;
            }
            String type;
            long rev;
            int colon = message.indexOf(':');
            if (colon < 0) {
                return;
            }
            type = message.substring(0, colon);
            rev = Long.parseLong(message.substring(colon + 1));
            switch (type) {
                case "committed" -> safeTick();
                case "fence" -> gate.arm();
                case "activate" -> revisionStore.loadSnapshot(rev)
                        .flatMap(raw -> Mono.fromCallable(() -> buildSnapshot(rev, raw)))
                        .doOnNext(snapshot -> activateLocal(snapshot, "ACTIVE"))
                        .subscribe(v -> { }, err -> log.warn("收到 activate 但本地装载失败：{}", err.toString()));
                case "abort" -> {
                    gate.open();
                    state = "ABORTED";
                }
                default -> log.debug("忽略未知路由协调消息：{}", message);
            }
        } catch (Exception e) {
            log.debug("处理路由协调消息失败 message={} err={}", message, e.toString());
        }
    }

    private Mono<Void> writeInstance(long targetRevision, String state, String error) {
        this.state = state;
        this.lastError = error;
        this.heartbeatAt = Instant.now();
        String json = instanceJson(targetRevision, state, error);
        return redis.opsForHash().put(RouteRevisionStore.INSTANCES_KEY, properties.instanceId(), json).then();
    }

    private Mono<Void> writeHeartbeat() {
        long target = Math.max(latestRevision, current == null ? 0 : current.revision());
        return writeInstance(target, state, lastError)
                .onErrorResume(e -> {
                    log.debug("路由实例心跳失败：{}", e.toString());
                    return Mono.empty();
                });
    }

    private void safeHeartbeat() {
        writeHeartbeat().subscribe();
    }

    private Mono<List<InstanceStatus>> instances(long latest) {
        return redis.opsForHash().entries(RouteRevisionStore.INSTANCES_KEY)
                .map(entry -> readInstance(entry.getValue().toString()))
                .collectList();
    }

    private InstanceStatus readInstance(String json) {
        try {
            return objectMapper.readValue(json, InstanceStatus.class);
        } catch (Exception e) {
            return localInstance("UNKNOWN", e.toString());
        }
    }

    private String instanceJson(long targetRevision, String state, String error) {
        try {
            return objectMapper.writeValueAsString(new InstanceStatus(
                    properties.instanceId(),
                    state,
                    current == null ? null : current.revision(),
                    targetRevision,
                    current == null ? null : current.checksum(),
                    lastLoadAt,
                    Instant.now(),
                    error));
        } catch (Exception e) {
            throw new IllegalStateException("序列化路由实例状态失败", e);
        }
    }

    private InstanceStatus localInstance(String state, String error) {
        return new InstanceStatus(properties.instanceId(), state,
                current == null ? null : current.revision(),
                latestRevision,
                current == null ? null : current.checksum(),
                lastLoadAt, heartbeatAt, error);
    }

    private static String checksum(TreeMap<String, String> sorted) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String combined = String.join("\n", sorted.values());
            digest.update(combined.getBytes(StandardCharsets.UTF_8));
            return "sha256:" + HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new IllegalStateException("计算路由快照 checksum 失败", e);
        }
    }

    private static String now() {
        return Instant.now().toString();
    }

    private static Long parseLong(String value) {
        return value == null || value.isBlank() ? null : Long.parseLong(value);
    }

    private static Instant parseInstant(String value) {
        try {
            return value == null || value.isBlank() ? null : Instant.parse(value);
        } catch (Exception e) {
            return null;
        }
    }

    public record CoordinationStatus(Long activeRevision,
                                     long latestRevision,
                                     String state,
                                     String error,
                                     int expectedInstances,
                                     Long localRevision,
                                     Instant lastLoadAt,
                                     Instant heartbeatAt,
                                     List<InstanceStatus> instances) {
        public CoordinationStatus {
            instances = instances == null ? List.of() : List.copyOf(instances);
        }
    }

    public record InstanceStatus(String instanceId,
                                 String state,
                                 Long activeRevision,
                                 long targetRevision,
                                 String checksum,
                                 Instant lastLoadAt,
                                 Instant heartbeatAt,
                                 String error) {
    }
}
