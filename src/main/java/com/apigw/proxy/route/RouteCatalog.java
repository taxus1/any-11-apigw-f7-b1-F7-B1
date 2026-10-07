package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.infrastructure.store.RouteRuleIndex;
import com.apigw.infrastructure.store.RouteStore;
import com.apigw.proxy.config.GatewayProxyProperties;
import com.apigw.proxy.resilience.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * 转发侧的「当前可用路由快照」。
 *
 * <p>转发是高频读路径，不在每个请求里访问 Redis。单实例/协调关闭时直接从 Redis 全量构造内存快照；
 * 多实例协调开启时，具体 revision、准备和切换由 {@link RouteCoordinator} 管理，这里只取不可变快照。
 *
 * <p>Redis 或协调组件一时异常时，只要已有一份好快照，就继续沿用上一份；只在从未加载成功时把
 * CONFIG_UNAVAILABLE 暴露给转发过滤器。
 */
@Slf4j
@Component
public class RouteCatalog {

    private final RouteStore routeStore;
    private final Duration ttl;
    private final RouteCoordinator coordinator;
    private final RouteRuleIndex ruleIndex;

    /**
     * 本机熔断器登记表是路由派生的运行时状态：协调模式由 {@link RouteCoordinator} 在快照激活后对账，
     * 单机模式由本目录在刷新后对账。用可选注入，保持单测里的轻量构造器不变。
     */
    @Autowired(required = false)
    private CircuitBreakerRegistry breakerRegistry;

    private volatile Snapshot snapshot;
    private Mono<List<GatewayRoute>> inflight;

    @Autowired
    public RouteCatalog(RouteStore routeStore,
                        GatewayProxyProperties properties,
                        RouteCoordinator coordinator,
                        RouteRuleIndex ruleIndex) {
        this.routeStore = routeStore;
        this.ttl = properties.routeRefreshInterval().multipliedBy(3);
        this.coordinator = coordinator;
        this.ruleIndex = ruleIndex;
    }

    /** 单元测试/关闭协调时使用的兼容构造器。 */
    public RouteCatalog(RouteStore routeStore, GatewayProxyProperties properties,
                        RouteCoordinator coordinator) {
        this(routeStore, properties, coordinator, null);
    }

    public RouteCatalog(RouteStore routeStore, GatewayProxyProperties properties) {
        this(routeStore, properties, null, null);
    }

    public Mono<RouteSnapshot> snapshot() {
        if (coordinator != null && coordinatorEnabled()) {
            return coordinator.snapshot();
        }
        return routes().map(routes -> new RouteSnapshot(0, "legacy", routes, null, java.time.Instant.now()));
    }

    private boolean coordinatorEnabled() {
        return coordinator != null && coordinator.isEnabled();
    }

    /**
     * 当前可用路由列表：新鲜就直接给；过期了拉一份新的并更新快照。
     */
    public Mono<List<GatewayRoute>> routes() {
        return baseRoutes().flatMap(this::applyRuleIndex);
    }

    /**
     * 装配时以规则索引为准：索引里存了这条路由的条件与动作，就直接用索引那份，
     * 省掉每次刷新都把整条路由 JSON 解一遍；索引里没有的，沿用路由自身那份。
     */
    private Mono<List<GatewayRoute>> applyRuleIndex(List<GatewayRoute> routes) {
        if (ruleIndex == null || routes == null || routes.isEmpty()) {
            return Mono.just(routes);
        }
        return ruleIndex.readAll()
                .map(index -> {
                    if (index.isEmpty()) {
                        return routes;
                    }
                    for (GatewayRoute r : routes) {
                        RouteRuleIndex.Rules rules = index.get(r.getRouteNo());
                        if (rules != null && !rules.isEmpty()) {
                            r.replaceRules(rules.conditionRules(), rules.actionRules());
                        }
                    }
                    return routes;
                })
                .onErrorResume(err -> Mono.just(routes));
    }

    private Mono<List<GatewayRoute>> baseRoutes() {
        if (coordinator != null && coordinatorEnabled()) {
            return coordinator.snapshot().map(RouteSnapshot::routes);
        }
        Snapshot current = this.snapshot;
        if (current != null && !isStale(current)) {
            return Mono.just(current.routes());
        }
        return refresh();
    }

    private boolean isStale(Snapshot current) {
        return System.currentTimeMillis() - current.loadedAtMs() > ttl.toMillis();
    }

    /** 配置变更事件：协调模式下由 revision pub/sub 驱动；兼容模式立即重载。 */
    @EventListener(RoutesChangedEvent.class)
    public void onRoutesChanged(RoutesChangedEvent event) {
        if (coordinator != null && coordinatorEnabled()) {
            coordinator.refresh().subscribe(
                    status -> log.debug("路由变更已触发协调，revision={}/{}",
                            status.activeRevision(), status.latestRevision()),
                    err -> log.warn("路由变更后协调失败，继续沿用上一份快照：{}", err.toString()));
            return;
        }
        log.debug("收到路由变更事件（{}），立即重载路由快照", event.reason());
        refresh().subscribe(
                list -> log.info("路由快照已按变更事件重载，当前可用路由 {} 条", list.size()),
                err -> log.warn("路由变更后重载失败，继续沿用上一份快照：{}", err.toString()));
    }

    /** 兜底轮询：协调模式由协调器自己 tick；兼容模式保留旧行为。 */
    @Scheduled(fixedDelayString = "${apigw.proxy.route-refresh-interval-ms:10000}")
    public void scheduledRefresh() {
        if (coordinator != null && coordinatorEnabled()) {
            return;
        }
        refresh().subscribe(
                list -> log.debug("路由快照定时刷新完成，当前可用路由 {} 条", list.size()),
                err -> log.debug("路由快照定时刷新失败，沿用上一份快照：{}", err.toString()));
    }

    /**
     * 单机模式下定期对账规则索引：权威路由已经删除、索引 field 却因当次清理失败而残留的，
     * 在这里按属主核对后收掉。它是「删除 best-effort 清理」之外的兜底，保证索引不越删越胀。
     * 协调模式不需要：转发走 revision 快照、不读索引，屏障/快照由协调器自行回收。
     */
    @Scheduled(fixedDelayString = "${apigw.proxy.rule-index-reconcile-interval-ms:60000}")
    public void reconcileRuleIndex() {
        if (coordinator != null && coordinatorEnabled() || ruleIndex == null) {
            return;
        }
        ruleIndex.reconcile()
                .onErrorResume(err -> {
                    log.warn("规则索引孤儿对账失败，下轮再试：{}", err.toString());
                    return Mono.just(0L);
                })
                .subscribe(removed -> {
                    if (removed > 0) {
                        log.info("规则索引对账回收 {} 条已删路由的无主记录", removed);
                    }
                });
    }

    /** 启动就绪后先拉一次；协调器自己的订阅/心跳在 PostConstruct 启动。 */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        if (coordinator != null && coordinatorEnabled()) {
            return;
        }
        refresh().subscribe(
                list -> log.info("路由快照预热完成，当前可用路由 {} 条", list.size()),
                err -> log.warn("路由快照预热失败（Redis 未就绪？），将在有请求时重试：{}", err.toString()));
    }

    /**
     * 强制拉一份新快照；失败保留旧快照。
     * 成功换上新快照后，回收本机里已从配置集合消失的派生熔断器。
     */
    public Mono<List<GatewayRoute>> refresh() {
        return load()
                .doOnNext(list -> {
                    this.snapshot = new Snapshot(List.copyOf(list), System.currentTimeMillis());
                    pruneBreakers();
                });
    }

    /**
     * 按当前全量权威路由对账本机熔断器（删除的路由、取消熔断的条目都收走）。
     * 必须拿<b>全量</b>（含停用/无条件的）路由来判断「配置里是否还有」，不能只拿可用快照，
     * 否则一条临时停用的路由会被误当成已删除。纯内存、失败不影响转发。
     */
    private void pruneBreakers() {
        if (breakerRegistry == null) {
            return;
        }
        routeStore.findAll()
                .collectList()
                .subscribe(all -> {
                    try {
                        breakerRegistry.pruneAbsent(all);
                    } catch (Exception e) {
                        log.debug("熔断器快照对账失败（不影响转发）：{}", e.toString());
                    }
                }, err -> log.debug("熔断器对账读取全量路由失败，跳过本轮：{}", err.toString()));
    }

    /** 测试装配：注入本机熔断器登记表。 */
    public void setBreakerRegistry(CircuitBreakerRegistry breakerRegistry) {
        this.breakerRegistry = breakerRegistry;
    }

    /**
     * 从 Redis 读全量，过滤出「启用且有条件」的路由。
     */
    private Mono<List<GatewayRoute>> load() {
        Mono<List<GatewayRoute>> task = inflight;
        if (task == null) {
            synchronized (this) {
                task = inflight;
                if (task == null) {
                    task = routeStore.findAll()
                            .filter(r -> Integer.valueOf(1).equals(r.getEnabled()))
                            .filter(r -> r.getConditions() != null && !r.getConditions().isEmpty())
                            .collectList()
                            .doFinally(sig -> {
                                synchronized (RouteCatalog.this) {
                                    inflight = null;
                                }
                            })
                            .onErrorResume(err -> {
                                log.warn("加载路由快照失败：{}", err.toString());
                                Snapshot last = this.snapshot;
                                return last == null ? Mono.error(err) : Mono.just(last.routes());
                            })
                            .cache();
                    inflight = task;
                }
            }
        }
        return task;
    }

    private record Snapshot(List<GatewayRoute> routes, long loadedAtMs) {
    }
}
