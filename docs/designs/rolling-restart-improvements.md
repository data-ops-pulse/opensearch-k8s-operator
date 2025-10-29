# Rolling Restart Design and Operations Guide

## Overview

This document provides comprehensive guidance for the rolling restart functionality in the OpenSearch Kubernetes operator. It covers both the design principles and operational procedures for managing rolling restarts in multi-AZ OpenSearch clusters.

The rolling restart implementation ensures cluster stability and availability during configuration updates by implementing a role-aware, quorum-preserving restart strategy with comprehensive failure handling and recovery mechanisms.

## Design Principles

The rolling restart implementation follows these core principles:

1. **Cluster Stability First** - Maintain OpenSearch cluster quorum and health during restarts
2. **Role-Aware Sequencing** - Restart nodes in an order that minimizes cluster impact
3. **Multi-AZ Support** - Handle node pools distributed across multiple availability zones
4. **One-at-a-Time Control** - Restart only one pod per reconciliation loop for precise control
5. **Failure Resilience** - Comprehensive retry mechanisms and recovery procedures
6. **Manual Intervention** - Support for pause, resume, and emergency recovery

## Architecture

### Global Candidate Rolling Restart Strategy

The operator implements a **comprehensive global candidate rolling restart strategy** that:

1. **Collects candidates across all StatefulSets** - Builds a global list of pods needing updates across all node types
2. **Applies intelligent candidate selection** - Prioritizes data nodes over master nodes, then sorts by StatefulSet name and highest ordinal
3. **Enforces master quorum preservation** - Ensures at least 2/3 masters are ready before restarting any master
4. **Restarts one pod at a time** - Only deletes one pod per reconciliation loop to maintain precise control

### Failure Handling and Recovery

The rolling restart implementation includes comprehensive failure handling:

#### Retry Mechanisms
- **Maximum retry count**: 5 attempts (configurable)
- **Exponential backoff**: `failureCount * retryInterval` (default: 30s)
- **Maximum failure duration**: 10 minutes before rollback
- **Automatic retry** for transient failures

#### Manual Intervention Support
- **Pause rolling restart**: `opensearch.opster.io/rolling-restart-paused=true`
- **Trigger recovery**: `opensearch.opster.io/rolling-restart-recovery=true`
- **Failure annotations** for debugging and monitoring

#### Emergency Recovery Procedures
- **Force shard allocation** to "all"
- **Reset failure counters** and clear annotations
- **Automatic status reset** to resume normal operation

## Implementation Design

### Global Candidate Collection

1. **Iterates through all node pools** to find StatefulSets with pending updates
2. **Identifies pods needing updates** by comparing `UpdateRevision` with pod labels
3. **Builds a global candidate list** across all StatefulSets and availability zones

### Intelligent Candidate Selection

Candidates are sorted using an algorithm that ensures optimal restart order:

1. **Prioritize data nodes over master nodes** for cluster stability
2. **Sort by StatefulSet name** for deterministic ordering across availability zones
3. **Within each StatefulSet, prefer highest ordinal** (drain from top)

### Master Quorum Preservation

Before restarting any master node:

1. **Calculate cluster-wide master quorum** across all master-eligible node pools
2. **Require at least 2/3 masters to be ready** before allowing any master restart
3. **Skip master restart** if insufficient quorum is available

### One-Pod-at-a-Time Restart

The operator deletes only one pod per reconciliation loop:

1. **Find the best candidate** from the sorted list
2. **Verify candidate eligibility** (quorum checks, health checks)
3. **Restart the specific pod** to trigger StatefulSet rolling update

## Design Benefits

### 1. **Comprehensive Cluster Stability**
- Prevents simultaneous restart of all master nodes
- Ensures data nodes restart before master nodes for optimal cluster health
- Maintains OpenSearch cluster quorum requirements
- Preserves cluster availability during configuration changes

### 2. **Multi-AZ and Multi-Node-Type Support**
- Handles all node types (master, data, coordinating, ingest) across multiple AZs
- Works with node provisioners like Karpenter that create separate node pools per AZ
- Ensures consistent restart behavior regardless of node distribution

### 3. **Predictable and Controlled Behavior**
- Clear restart order: data nodes → coordinating nodes → master nodes
- One pod restart at a time for precise control
- Maintains cluster health and availability during updates

### 4. **Backward Compatibility**
- No changes to existing API or configuration
- Works with existing cluster configurations

## Operational Procedures

### Failure Detection

#### Automatic Failure Detection
The rolling restart reconciler automatically detects and handles various failure scenarios:

- **StatefulSet retrieval failures**
- **Pod discovery failures** 
- **Cluster health check failures**
- **Pod preparation failures**
- **Pod deletion failures**
- **Shard allocation failures**

#### Failure Indicators

When a rolling restart fails, the following indicators will be present:

##### 1. Cluster Annotations
- `opensearch.opster.io/rolling-restart-failed: "true"` - Indicates rolling restart has failed
- `opensearch.opster.io/rolling-restart-failure-type` - Specific type of failure
- `opensearch.opster.io/rolling-restart-error` - Detailed error message
- `opensearch.opster.io/rolling-restart-failure-time` - Timestamp of failure
- `opensearch.opster.io/rolling-restart-failure-count` - Number of retry attempts

##### 2. Component Status
- Status field set to "Failed" in the RollingRestart component
- Description field contains failure details and retry count

##### 3. Kubernetes Events
- Warning events with reason "RollingRestart" are emitted
- Events contain detailed error information for debugging

### Recovery Procedures

#### 1. Immediate Assessment

First, assess the current state of the cluster:

- **Check cluster status** - Review the OpenSearchCluster resource status
- **Check component status** - Verify RollingRestart component status
- **Check cluster health** - Query OpenSearch cluster health API
- **Check pod status** - Verify master and data pod readiness

#### 2. Identify Failure Type

Check the failure type annotation to understand what went wrong by examining the `opensearch.opster.io/rolling-restart-failure-type` annotation.

Common failure types and their meanings:

| Failure Type | Description | Recovery Action |
|--------------|-------------|-----------------|
| `statefulset_retrieval_failed` | Cannot retrieve StatefulSet | Check RBAC, API connectivity |
| `pod_deletion_failed` | Cannot delete pod | Check pod status, manual deletion |
| `cluster_health_check_failed` | Cluster health check failed | Check cluster health, fix issues |
| `pod_preparation_failed` | Pod preparation failed | Check shard allocation, cluster state |
| `shard_allocation_failed` | Shard allocation failed | Check cluster settings, fix allocation |

#### 3. Recovery Methods

##### Method 1: Automatic Recovery (Recommended)

If the failure was temporary and the cluster is now healthy:

1. **Trigger automatic recovery** by setting the `opensearch.opster.io/rolling-restart-recovery=true` annotation
2. **Monitor recovery** by watching the cluster status

The operator will:
1. Force shard allocation to "all"
2. Reset failure counters
3. Clear failure annotations
4. Resume normal operation

##### Method 2: Manual Pod Deletion

If specific pods are stuck and need manual deletion:

1. **Identify stuck pods** - Look for pods in Terminating, Error, or CrashLoopBackOff states
2. **Force delete stuck pods** - Use force deletion with zero grace period
3. **Wait for pod recreation** - Monitor pod status until new pods are ready

##### Method 3: StatefulSet Recreation

If StatefulSets are in a bad state:

1. **Delete affected StatefulSets** - Remove master and data StatefulSets
2. **Wait for recreation** - Monitor StatefulSet recreation by the operator

#### 4. Verification

After recovery, verify the cluster is healthy:

1. **Check cluster health** - Query OpenSearch cluster health API
2. **Check all pods are ready** - Verify pod readiness status
3. **Check component status** - Verify RollingRestart component status
4. **Check annotations are cleared** - Ensure failure annotations are removed
