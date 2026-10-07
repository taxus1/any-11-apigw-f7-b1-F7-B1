package com.apigw.proxy.gray;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GrayGroup;
import com.apigw.proxy.gray.GrayPlan.Slot;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 灰度分流的运行时入口：匹配到路由之后、转发之前，由它决定这笔请求打哪个上游。
 *
 * 决策顺序是本功能最关键的口径，<b>标记永远优先于权重</b>：
 * <pre>
 *   请求带灰度标记头？
 *     ├─ 值精确命中某个组认领的标记（大小写/空格一个字符都不差）→ 直达该组，与权重无关
 *     └─ 没带 / 值不对 / 大小写不一致 / 多空格 / 像却对不上 → 当作没带标记
 *   没命中标记 → 在「权重 &gt; 0」的组之间做平滑加权轮询；
 *                权重 0 的组不参与，但配置留着，改权重新快照一到就能放量
 * </pre>
 *
 * 因此：
 * - 新版只占一成（10）也分不走带标的请求——标记该去新版还是新版，哪怕新版权重是 0；
 * - 100/0、0/100 这种极端配置：标记各回各的组；没标记的永远去 100 那个组，行为确定不漂移；
 * - 同一配置期间标记是确定映射，无标记请求按确定性轮询推进，结果说得清。
 *
 * 性能与新鲜度：分流用的 {@link GrayPlan} 在首次用到时编译并缓存，热路径上只做一次头查找
 * + 一次微秒级整数轮询，不解析 JSON、不碰 Redis。缓存键是路由编号，新鲜度用路由的
 * 乐观锁版本号判定——配置每保存一次版本必 +1：
 * - 快照 10s 兜底重载出来的是同版本的新对象 → 复用同一计划，轮询计数器不被重置
 *   （低流量下若每次重载都清零，10s 只有一两个请求时小权重组会永远选不上）；
 * - 权重/标记/上游一改、版本一变 → 立刻编译新计划，不存在「结果缓死、改了迟迟不生效」。
 * 缓存有界（{@link #MAX_PLANS}，access-order LRU）；但删除不能只等 LRU——同编号重建版本从 0
 * 重新计，旧计划会被新路由误认成同版本，所以路由删除/快照切换时由对账逻辑显式摘除。
 */
@Component
public class GrayReleaseSelector {

    /** 计划缓存上限：路由数远超它时按最近使用淘汰；正常规模下所有在用路由都常驻。 */
    private static final int MAX_PLANS = 512;

    private final GrayProperties properties;

    /** 路由编号 -> 某版本的已编译计划；access-order LRU，synchronized 兜住链表调整。 */
    private final Map<String, CachedPlan> plans = java.util.Collections.synchronizedMap(
            new java.util.LinkedHashMap<>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CachedPlan> eldest) {
                    return size() > MAX_PLANS;
                }
            });

    public GrayReleaseSelector(GrayProperties properties) {
        this.properties = properties;
    }

    /** 没配灰度分组的路由：null，调用方继续用主上游，灰度逻辑完全不介入。 */
    public GrayTarget select(GatewayRoute route, ServerHttpRequest request) {
        if (!route.hasGrayGroups()) {
            return null;
        }
        List<GrayGroup> groups = route.getGrayGroups();
        GrayPlan plan = planFor(route, groups);

        // 1) 标记精确命中优先：HTTP 头名大小写不敏感（getFirst 已处理），头值原样比较
        String tagValue = request.getHeaders().getFirst(properties.tagHeader());
        int taggedIndex = plan.matchTag(tagValue);
        if (taggedIndex >= 0) {
            GrayGroup g = groups.get(taggedIndex);
            return new GrayTarget(g.getGroupName(), g.getUpstream(), g.getWeight(), true);
        }

        // 2) 无标记（或标记对不上）：平滑加权轮询，只在 weight>0 的组里选
        Slot slot = plan.pickByWeight();
        return new GrayTarget(slot.name(), slot.upstream(), slot.weight(), false);
    }

    /** 取（或按新版本编译）这份路由配置对应的计划。 */
    private GrayPlan planFor(GatewayRoute route, List<GrayGroup> groups) {
        String routeNo = route.getRouteNo();
        int version = route.getVersion() == null ? 0 : route.getVersion();
        CachedPlan cached = plans.get(routeNo);
        if (cached != null && cached.version() == version) {
            return cached.plan();
        }
        GrayPlan compiled = GrayPlan.compile(routeNo, groups);
        plans.put(routeNo, new CachedPlan(version, compiled));
        return compiled;
    }

    /** 缓存可见的诊断：当前驻留的计划数（测试/监控用）。 */
    public int cachedPlanCount() {
        return plans.size();
    }

    /**
     * 路由删除时显式摘除它的编译计划。
     *
     * <p>不能只依赖 LRU 惰性淘汰：删除后同编号重建是一条版本仍从 0 起算的全新路由，
     * 缓存键是编号、新鲜度只比版本号，若不摘除，新路由在首次更新前会直接复用上一任的分流计划，
     * 把灰度流量按老分组/老权重/老上游分走。
     */
    public void evict(String routeNo) {
        plans.remove(routeNo);
    }

    /**
     * 快照切换后的对账：只保留当前生效快照里确实配了灰度分组的路由计划，
     * 删除、停用灰度、还没激活到本实例的路由计划一律摘掉，缓存不随历史只增不减。
     */
    public void reconcileTo(List<GatewayRoute> liveRoutes) {
        Set<String> live = new java.util.HashSet<>();
        if (liveRoutes != null) {
            for (GatewayRoute r : liveRoutes) {
                if (r.hasGrayGroups()) {
                    live.add(r.getRouteNo());
                }
            }
        }
        plans.keySet().retainAll(live);
    }

    private record CachedPlan(int version, GrayPlan plan) {
    }
}
