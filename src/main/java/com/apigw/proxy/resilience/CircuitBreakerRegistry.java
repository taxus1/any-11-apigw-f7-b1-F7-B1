package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.GatewayRoute;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本机熔断器登记表：按「路由编号 × 目标上游」各持一个 {@link CircuitBreaker}，
 * 纯进程内存，三台网关互不共享（各自独立熔断，符合需求）。
 *
 * 粒度选「路由 × 上游」而不是只按路由：灰度路由的不同分组是不同上游，
 * canary 组挂了不该把 stable 组也熔断。
 *
 * 热刷新：路由配置改了以后，用新参数的熔断器<b>只在参数真的变了</b>时替换
 * （重置窗口/状态，语义等同「按新规矩重新观察」）；参数没变就沿用原熔断器，
 * 一次普通的路由改名/动作调整不会把已经积累的故障观察清掉。
 * 路由不再启用熔断时直接摘除（条目惰性回收，数量上限就是启用熔断的路由×上游数）。
 */
@Component
public class CircuitBreakerRegistry {

    private record Key(String routeNo, String upstream) {
    }

    private record Entry(CircuitBreakerPolicy policy, CircuitBreaker breaker) {
    }

    private final ConcurrentHashMap<Key, Entry> breakers = new ConcurrentHashMap<>();

    /**
     * 取（必要时建）本笔请求要用的熔断器。
     *
     * @param policy 路由当前生效的熔断策略（调用方已确认该路由启用了熔断）；传 null 返回 null
     */
    public CircuitBreaker obtain(String routeNo, String upstream, CircuitBreakerPolicy policy) {
        if (policy == null) {
            return null;
        }
        Key key = new Key(routeNo, upstream);
        Entry existing = breakers.get(key);
        if (existing != null && existing.breaker().sameConfig(policy)) {
            return existing.breaker();
        }
        // 参数变了（或第一次见）：并发放置时用 merge 保证同 key 只有一个生效
        CircuitBreaker created = new CircuitBreaker(routeNo + "@" + upstream, policy);
        Entry merged = breakers.merge(key, new Entry(policy, created),
                (oldEntry, newEntry) -> oldEntry.breaker().sameConfig(policy) ? oldEntry : newEntry);
        return merged.breaker();
    }

    /** 路由停用熔断后摘条目（下次再开按全新窗口起算）。测试也用它复位。 */
    public void remove(String routeNo, String upstream) {
        breakers.remove(new Key(routeNo, upstream));
    }

    /**
     * 快照切换后对账：把「当前可用路由集合里已经不存在」的熔断器摘掉。
     *
     * <p>这是路由派生的本机运行时状态，不随删除 API 清理（删除发生在管理写路径，
     * 熔断器只活在各自网关进程里），而是在每次换上新快照时以新快照为基准收敛：
     * 路由被删、或某上游（含灰度分组上游）从配置里消失，对应条目就收掉，
     * 避免长期运行后登记表无限膨胀。同编号重建是新配置重新 {@link #obtain}，
     * 旧熔断器不会跨代复用——它持有的是上一代的策略与故障窗口。
     *
     * <p>纯内存操作，失败不影响转发。
     *
     * @return 本次摘掉的条目数
     */
    public int pruneAbsent(List<GatewayRoute> activeRoutes) {
        Set<Key> live = new HashSet<>();
        for (GatewayRoute route : activeRoutes) {
            if (route.circuitBreakerPolicy() == null) {
                continue;
            }
            live.add(new Key(route.getRouteNo(), route.getUpstream()));
            if (route.getGrayGroups() != null) {
                route.getGrayGroups().forEach(g ->
                        live.add(new Key(route.getRouteNo(), g.getUpstream())));
            }
        }
        int before = breakers.size();
        breakers.keySet().removeIf(key -> !live.contains(key));
        return before - breakers.size();
    }

    /** 当前持有的熔断器数量（测试/监控用）。 */
    public int size() {
        return breakers.size();
    }
}
