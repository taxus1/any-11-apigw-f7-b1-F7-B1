package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.proxy.gray.GrayReleaseSelector;
import com.apigw.proxy.resilience.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 路由派生的「本机运行时状态」回收器：每次有新快照在本实例激活后调用一次 {@link #reconcile}。
 *
 * <p>这些状态都是<b>纯进程内存、可由当前配置随时重建</b>的派生物，路由删除时必须收干净：
 * <ul>
 *   <li>{@link CircuitBreakerRegistry}：按「路由 × 上游」积累的故障窗口/熔断状态。
 *       留着只会白白占内存；同编号重建且上游相同时还会把上一任的熔断状态错误继承给新路由。</li>
 *   <li>{@link GrayReleaseSelector}：按「路由编号 + 版本」编译缓存的分流计划。
 *       同编号重建版本从 0 起算，不显式摘除就会被新路由当成同版本计划复用，按老分组老权重分流。</li>
 * </ul>
 *
 * <p>口径是「以当前生效快照对账」而不是「按删除事件点名」：多台网关之间事件可能乱序/丢失，
 * 快照才是各台共同对齐的权威视图。对账后存活的运行时状态集合 == 快照实际需要的集合，
 * 无论删除事件送达几次、漏送一次，结果都收敛，不会越删越胀。
 *
 * <p>什么<b>不</b>在这清：不可变 revision 快照（多卡一致切换与回滚的依据，由 revision 提交按
 * 保留份数/TTL 轮转）、访问流水（合规审计留档）、协调屏障/实例心跳（协调过程状态，自带 TTL）。
 */
@Slf4j
@Component
public class RouteRuntimeStateReaper {

    private final CircuitBreakerRegistry breakerRegistry;
    private final GrayReleaseSelector graySelector;

    public RouteRuntimeStateReaper(CircuitBreakerRegistry breakerRegistry,
                                   GrayReleaseSelector graySelector) {
        this.breakerRegistry = breakerRegistry;
        this.graySelector = graySelector;
    }

    /**
     * 按刚激活的快照对账本机全部按路由派生的运行时状态。
     * 纯内存操作、不抛异常（回收失败绝不能影响转发；下一次快照切换会再次对账收敛）。
     */
    public void reconcile(List<GatewayRoute> activeRoutes) {
        try {
            breakerRegistry.reconcile(CircuitBreakerRegistry.liveKeysOf(activeRoutes));
            graySelector.reconcileTo(activeRoutes);
        } catch (Exception e) {
            log.warn("路由运行时状态对账失败，等待下次快照切换再次对账：{}", e.toString());
        }
    }
}
