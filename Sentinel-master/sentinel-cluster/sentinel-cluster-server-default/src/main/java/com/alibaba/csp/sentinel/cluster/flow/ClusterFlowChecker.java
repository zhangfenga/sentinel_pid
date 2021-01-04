/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.cluster.flow;

import com.alibaba.csp.sentinel.cluster.TokenResultStatus;
import com.alibaba.csp.sentinel.cluster.TokenResult;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.statistic.ClusterMetricStatistics;
import com.alibaba.csp.sentinel.cluster.flow.statistic.limit.GlobalRequestLimiter;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.flow.statistic.data.ClusterFlowEvent;
import com.alibaba.csp.sentinel.cluster.flow.statistic.metric.ClusterMetric;
import com.alibaba.csp.sentinel.cluster.server.log.ClusterServerStatLogUtil;
import com.alibaba.csp.sentinel.slots.block.ClusterRuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;

/**
 * Flow checker for cluster flow rules.
 *
 * @author Eric Zhao
 * @since 1.4.0
 */
final class ClusterFlowChecker {

    private static double calcGlobalThreshold(FlowRule rule) {
        double count = rule.getCount();
        switch (rule.getClusterConfig().getThresholdType()) {
            //如果是集群模式
            case ClusterRuleConstant.FLOW_THRESHOLD_GLOBAL:
                return count;
                //如果是单机模式
            case ClusterRuleConstant.FLOW_THRESHOLD_AVG_LOCAL:
            default:
                //获取这个规则对应的命名空间有多少连接
                int connectedCount = ClusterFlowRuleManager.getConnectedCount(rule.getClusterConfig().getFlowId());
                return count * connectedCount;
        }
    }

    static boolean allowProceed(long flowId) {
        String namespace = ClusterFlowRuleManager.getNamespace(flowId);
        return GlobalRequestLimiter.tryPass(namespace);
    }

    static TokenResult acquireClusterToken(/*@Valid*/ FlowRule rule, int acquireCount, boolean prioritized) {
        Long id = rule.getClusterConfig().getFlowId();

        if (!allowProceed(id)) {
            return new TokenResult(TokenResultStatus.TOO_MANY_REQUEST);
        }

        ClusterMetric metric = ClusterMetricStatistics.getMetric(id);
        if (metric == null) {
            return new TokenResult(TokenResultStatus.FAIL);
        }

        //每秒最后通过的qps
        double latestQps = metric.getAvg(ClusterFlowEvent.PASS);
        //这个规则所能通过的所有qps(先判断是单机还是集群，然后乘以配置上的qps等于一共能通过多少qps)
        double globalThreshold = calcGlobalThreshold(rule) * ClusterServerConfigManager.getExceedCount();
        //剩余的请求数
        double nextRemaining = globalThreshold - latestQps - acquireCount;

        if (nextRemaining >= 0) {
            // TODO: checking logic and metric operation should be separated.
            metric.add(ClusterFlowEvent.PASS, acquireCount);
            metric.add(ClusterFlowEvent.PASS_REQUEST, 1);
            if (prioritized) {
                // Add prioritized pass.
                metric.add(ClusterFlowEvent.OCCUPIED_PASS, acquireCount);
            }
            // Remaining count is cut down to a smaller integer.
            return new TokenResult(TokenResultStatus.OK)
                .setRemaining((int) nextRemaining)
                .setWaitInMs(0);
        } else {
            if (prioritized) {
                // Try to occupy incoming buckets.
                //试着占领进入的桶
                //获取正在等待进入下一次桶的平均请求
                double occupyAvg = metric.getAvg(ClusterFlowEvent.WAITING);
                //如果这个值小于等于下一个时间段内可以请求通过的最大值
                if (occupyAvg <= ClusterServerConfigManager.getMaxOccupyRatio() * globalThreshold) {
                    //就让这些请求通过
                    int waitInMs = metric.tryOccupyNext(ClusterFlowEvent.PASS, acquireCount, globalThreshold);
                    // waitInMs > 0 indicates pre-occupy incoming buckets successfully.
                    if (waitInMs > 0) {
                        ClusterServerStatLogUtil.log("flow|waiting|" + id);
                        return new TokenResult(TokenResultStatus.SHOULD_WAIT)
                            .setRemaining(0)
                            .setWaitInMs(waitInMs);
                    }
                    // Or else occupy failed, should be blocked.
                }
            }
            // Blocked.
            //阻塞
            metric.add(ClusterFlowEvent.BLOCK, acquireCount);
            metric.add(ClusterFlowEvent.BLOCK_REQUEST, 1);
            ClusterServerStatLogUtil.log("flow|block|" + id, acquireCount);
            ClusterServerStatLogUtil.log("flow|block_request|" + id, 1);
            if (prioritized) {
                // Add prioritized block.
                metric.add(ClusterFlowEvent.OCCUPIED_BLOCK, acquireCount);
                ClusterServerStatLogUtil.log("flow|occupied_block|" + id, 1);
            }

            return blockedResult();
        }
    }

    private static TokenResult blockedResult() {
        return new TokenResult(TokenResultStatus.BLOCKED)
            .setRemaining(0)
            .setWaitInMs(0);
    }

    private ClusterFlowChecker() {}
}
