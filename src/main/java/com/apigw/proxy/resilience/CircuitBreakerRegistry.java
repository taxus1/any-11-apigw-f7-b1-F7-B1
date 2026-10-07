package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.GatewayRoute;
import org.springframework.stereotype.Component;

import java.util.HashSet;
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
     * 整条路由删除（或同编号重建后上游全换）时，把它名下所有上游的熔断器一次摘净。
     * 熔断器是纯进程内的运行时状态，删路由后没有任何请求还会拿到这些 key；留着只会让内存越攒越多，
     * 并且同编号重建时若上游恰好相同，可能错误继承上一任路由积累的熔断状态。
     */
    public void removeRoute(String routeNo) {
        breakers.keySet().removeIf(key -> key.routeNo().equals(routeNo));
    }

    /**
     * 快照切换后的对账：以「当前生效快照里实际启用熔断的 路由×上游」为唯一存活集合，
     * 其余条目全部摘除。删除/停用/改掉上游的路由不需要调用方逐个点名，一次对账全部收干净，
     * 保证登记表规模只与当前生效配置相关，不会随历史变更只增不减。
     */
    public void reconcile(Set<Key> liveKeys) {
        breakers.keySet().retainAll(liveKeys);
    }

    /** 计算当前生效快照里启用熔断的「路由 × 上游」存活集合（含灰度分组的各上游）。 */
    public static Set<Key> liveKeysOf(java.util.List<GatewayRoute> routes) {
        Set<Key> live = new HashSet<>();
        if (routes == null) {
            return live;
        }
        for (GatewayRoute route : routes) {
            CircuitBreakerPolicy policy = route.circuitBreakerPolicy();
            if (policy == null) {
                continue;
            }
            live.add(new Key(route.getRouteNo(), route.getUpstream()));
            if (route.getGrayGroups() != null) {
                route.getGrayGroups().forEach(g -> live.add(new Key(route.getRouteNo(), g.getUpstream())));
            }
        }
        return live;
    }

    /** 路由编号 × 目标上游：快照对账的存活 key 类型，与内部熔断登记表同形。 */
    public record Key(String routeNo, String upstream) {
    }
}
