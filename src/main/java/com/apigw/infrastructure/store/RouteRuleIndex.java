package com.apigw.infrastructure.store;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RuleTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则索引：把每条路由的匹配条件与转发动作单独存一份，匹配装配时以它为准，
 * 省得每次刷新都要把整条路由 JSON 解一遍的开销。
 *
 * <p><b>它只是派生缓存，不是第二份真源</b>：权威配置只有 {@code apigw:routes}，
 * 协调模式下转发用的不可变快照也直接来自 revision 快照，根本不读这里。
 * 因此它的维护一律 best-effort：写索引失败不能拖垮提交，漏写/错写最坏只是回落到路由自带的那份规则。
 *
 * <h3>归属与删除</h3>
 * 索引是 Hash（field = routeNo），field 里带 {@code ownerId}（创建时分配的路由内部 id）：
 * <ul>
 *   <li>写入永远是<b>整份覆盖</b>：新建不带任何历史规则，杜绝「同编号删了又建，老条件老动作接到新路由上」；</li>
 *   <li>删除走 {@link #removeIfOwner} 的属主栅栏：只有索引确属被删的那一代路由时才清，
 *       「刚删就建」时新建那一代不会被迟到的清理误删；</li>
 *   <li>{@link #reconcile} 兜底清孤儿：field 对应的权威路由已经不在了，就按属主核对后收掉，
 *       保证这个 Hash 不会越删越胀。</li>
 * </ul>
 */
@Component
public class RouteRuleIndex {

    public static final String RULES_KEY = "apigw:route:rules";

    /**
     * 属主栅栏删除：field 的 ownerId 与被删路由的 id 一致才 HDEL。
     * 老数据没有 ownerId 也允许删——那条索引必然属于更早的一代。
     * 若 field 已不存在或已换成新路由的 ownerId，返回 0 什么都不动。
     */
    private static final RedisScript<Long> REMOVE_IF_OWNER_SCRIPT = new DefaultRedisScript<>(
            "local raw = redis.call('HGET', KEYS[1], ARGV[1]) "
                    + "if raw == false then return 0 end "
                    + "local owner = string.match(raw, '\"ownerId\"%s*:%s*\"([^\"]*)\"') "
                    + "if owner ~= false and owner ~= nil and owner ~= ARGV[2] then return 0 end "
                    + "return redis.call('HDEL', KEYS[1], ARGV[1])",
            Long.class);

    /**
     * 单 field 孤儿/旧代回收（在 Redis 内交叉核对权威 Hash，天然扛住「读到对账名单之后又发生重建」）：
     * - 索引不存在：0；
     * - 权威路由不存在（已删）：删掉索引；
     * - 权威路由还在、且索引 ownerId 就是权威路由当前 id（同一代）：保留，返回 0；
     * - 权威路由还在、但索引属主是上一代（新建时索引没写成）：删掉，
     *   装配回落路由自带的那份规则，下次提交会重建索引。
     */
    private static final RedisScript<Long> RECONCILE_FIELD_SCRIPT = new DefaultRedisScript<>(
            "local raw = redis.call('HGET', KEYS[1], ARGV[1]) "
                    + "if raw == false then return 0 end "
                    + "local indexOwner = string.match(raw, '\"ownerId\"%s*:%s*\"([^\"]*)\"') "
                    + "local auth = redis.call('HGET', KEYS[2], ARGV[1]) "
                    + "if auth ~= false then "
                    + "  local authOwner = string.match(auth, '\"id\"%s*:%s*\"([^\"]*)\"') "
                    + "  if indexOwner ~= false and indexOwner ~= nil "
                    + "     and authOwner ~= false and authOwner ~= nil "
                    + "     and indexOwner == authOwner then return 0 end "
                    + "end "
                    + "return redis.call('HDEL', KEYS[1], ARGV[1])",
            Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RouteRuleIndex(ReactiveStringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    /** 覆盖写一条路由的规则（带属主 id）。 */
    public Mono<Void> write(String routeNo, Rules rules) {
        return redis.opsForHash().put(RULES_KEY, routeNo, serialize(rules)).then();
    }

    /** 读一条路由的规则；没有则为空。 */
    public Mono<Rules> read(String routeNo) {
        return redis.opsForHash().get(RULES_KEY, routeNo)
                .map(v -> deserialize(String.valueOf(v)));
    }

    /** 一次读全部规则（装配时一次 HGETALL）。 */
    public Mono<Map<String, Rules>> readAll() {
        return redis.opsForHash().entries(RULES_KEY)
                .collectMap(e -> String.valueOf(e.getKey()),
                        e -> deserialize(String.valueOf(e.getValue())),
                        LinkedHashMap::new);
    }

    /**
     * 清掉一条路由的规则，但只在索引仍属于 {@code ownerRouteId} 这一代时清。
     *
     * <p>典型时序：删除提交成功后，同编号立刻被重建（新 ownerId）；删除方的迟到清理跑到时，
     * 属主对不上就直接收手，绝不把新路由的规则抹掉。
     *
     * @return 是否真的删掉了（false = 没有该 field，或它已经属于新一代路由）
     */
    public Mono<Boolean> removeIfOwner(String routeNo, String ownerRouteId) {
        return redis.execute(REMOVE_IF_OWNER_SCRIPT,
                        List.of(RULES_KEY),
                        List.of(routeNo, ownerRouteId == null ? "" : ownerRouteId))
                .next()
                .map(n -> n != null && n > 0);
    }

    /**
     * 孤儿/旧代对账：扫一遍索引，每个 field 在 Redis 内与权威 {@code apigw:routes} 交叉核对后，
     * 决定保留（同一代）还是删除（权威已不存在，或权威已是同号的新一代）。
     *
     * <p>用它兜住「删除成功但清理那一步失败/没跑到」的残留，而不是把清理塞进权威提交里——
     * 清理失败只影响派生缓存，不能反过来让删除失败。判定在一个 Lua 里完成，
     * 不会因为「读名单」和「执行删除」之间夹了一次同号重建而误清新一代。
     *
     * @return 清掉的孤儿/旧代 field 数量
     */
    public Mono<Long> reconcile() {
        return redis.opsForHash().keys(RULES_KEY)
                .flatMap(field -> redis.execute(RECONCILE_FIELD_SCRIPT,
                                List.of(RULES_KEY, RouteStore.ROUTES_KEY),
                                List.of(String.valueOf(field)))
                        .next().map(v -> v == null ? 0L : ((Number) v).longValue()))
                .reduce(0L, Long::sum);
    }

    private String serialize(Rules rules) {
        try {
            return objectMapper.writeValueAsString(rules);
        } catch (Exception e) {
            throw new BizException("规则索引序列化失败：" + e.getMessage());
        }
    }

    private Rules deserialize(String json) {
        try {
            Rules r = objectMapper.readValue(json, Rules.class);
            return r == null ? new Rules() : r;
        } catch (Exception e) {
            throw new BizException("规则索引反序列化失败：" + e.getMessage());
        }
    }

    /** 索引里存的形状：属主路由 id + 匹配条件 + 转发动作。 */
    public static class Rules {
        /** 这份索引属于哪一代路由（路由内部 id）；老数据没有这个字段，按「无主」处理。 */
        public String ownerId;
        public List<RuleItem> conditions = new ArrayList<>();
        public List<RuleItem> actions = new ArrayList<>();

        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean isEmpty() {
            return conditions.isEmpty() && actions.isEmpty();
        }

        /** 从一条路由抽出规则，属主记为该路由的内部 id。 */
        public static Rules of(GatewayRoute route) {
            Rules r = new Rules();
            r.ownerId = route.getId();
            for (GatewayRule c : route.getConditions()) {
                r.conditions.add(RuleItem.from(c));
            }
            for (GatewayRule a : route.getActions()) {
                r.actions.add(RuleItem.from(a));
            }
            return r;
        }

        public List<GatewayRule> conditionRules() {
            List<GatewayRule> out = new ArrayList<>();
            for (RuleItem i : conditions) {
                GatewayRule g = i.toDomain();
                g.setRuleKind(RuleTypes.KIND_CONDITION);
                out.add(g);
            }
            return out;
        }

        public List<GatewayRule> actionRules() {
            List<GatewayRule> out = new ArrayList<>();
            for (RuleItem i : actions) {
                GatewayRule g = i.toDomain();
                g.setRuleKind(RuleTypes.KIND_ACTION);
                out.add(g);
            }
            return out;
        }
    }

    /** 单条规则的落盘形状（与路由 JSON 里的子项字段一致）。 */
    public static class RuleItem {
        public String id;
        public String stage;
        public String type;
        public String name;
        public String value;
        public Integer sortNo;

        static RuleItem from(GatewayRule g) {
            RuleItem d = new RuleItem();
            d.id = g.getId();
            d.stage = g.getStage();
            d.type = g.getType();
            d.name = g.getName();
            d.value = g.getValue();
            d.sortNo = g.getSortNo();
            return d;
        }

        GatewayRule toDomain() {
            return GatewayRule.create(stage, type, name, value, sortNo);
        }
    }
}
