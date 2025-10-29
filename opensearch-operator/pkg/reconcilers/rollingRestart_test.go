package reconcilers

import (
	"context"
	"fmt"
	"time"

	opsterv1 "github.com/Opster/opensearch-k8s-operator/opensearch-operator/api/v1"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/mocks/github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/k8s"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/helpers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ = Describe("RollingRestart Reconciler", func() {
	var (
		mockClient *k8s.MockK8sClient
		instance   *opsterv1.OpenSearchCluster
		reconciler *RollingRestartReconciler
	)

	BeforeEach(func() {
		mockClient = k8s.NewMockK8sClient(GinkgoT())
		instance = &opsterv1.OpenSearchCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
			Spec: opsterv1.ClusterSpec{
				General: opsterv1.GeneralConfig{
					ServiceName: "test-cluster",
					Version:     "2.14.0",
				},
				NodePools: []opsterv1.NodePool{
					{
						Component: "master",
						Replicas:  3,
						Roles:     []string{"cluster_manager"},
					},
					{
						Component: "data",
						Replicas:  3,
						Roles:     []string{"data"},
					},
				},
			},
			Status: opsterv1.ClusterStatus{
				Initialized: true,
			},
		}
		reconciler = &RollingRestartReconciler{
			client:   mockClient,
			ctx:      context.Background(),
			instance: instance,
		}
	})

	Describe("parseOrdinalFromName", func() {
		Context("with valid pod name", func() {
			It("should extract ordinal correctly", func() {
				Expect(parseOrdinalFromName("test-cluster-master-0")).To(Equal(0))
				Expect(parseOrdinalFromName("test-cluster-data-2")).To(Equal(2))
				Expect(parseOrdinalFromName("test-cluster-master-10")).To(Equal(10))
			})
		})

		Context("with invalid pod name", func() {
			It("should return -1", func() {
				Expect(parseOrdinalFromName("invalid-name")).To(Equal(-1))
				Expect(parseOrdinalFromName("test-cluster-master-abc")).To(Equal(-1))
				Expect(parseOrdinalFromName("")).To(Equal(-1))
			})
		})
	})

	Describe("countMasters", func() {
		Context("with master node pools", func() {
			BeforeEach(func() {
				// Mock StatefulSets for master nodes
				masterSts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-master",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.StatefulSetStatus{
						ReadyReplicas: 3,
					},
				}
				mockClient.EXPECT().GetStatefulSet("test-cluster-master", "default").Return(*masterSts, nil)
			})

			It("should count masters correctly", func() {
				total, ready, err := reconciler.countMasters()
				Expect(err).ToNot(HaveOccurred())
				Expect(total).To(Equal(int32(3)))
				Expect(ready).To(Equal(int32(3)))
			})
		})
	})

	Describe("hasManagerRole", func() {
		Context("with cluster_manager role", func() {
			It("should return true", func() {
				nodePool := opsterv1.NodePool{
					Component: "master",
					Roles:     []string{"cluster_manager"},
				}
				Expect(helpers.HasManagerRole(&nodePool)).To(BeTrue())
			})
		})

		Context("with master role", func() {
			It("should return true", func() {
				nodePool := opsterv1.NodePool{
					Component: "master",
					Roles:     []string{"master"},
				}
				Expect(helpers.HasManagerRole(&nodePool)).To(BeTrue())
			})
		})

		Context("with data role only", func() {
			It("should return false", func() {
				nodePool := opsterv1.NodePool{
					Component: "data",
					Roles:     []string{"data"},
				}
				Expect(helpers.HasManagerRole(&nodePool)).To(BeFalse())
			})
		})

		Context("with no roles", func() {
			It("should return false", func() {
				nodePool := opsterv1.NodePool{
					Component: "coordinating",
					Roles:     []string{},
				}
				Expect(helpers.HasManagerRole(&nodePool)).To(BeFalse())
			})
		})
	})

	Describe("hasDataRole", func() {
		Context("with data role", func() {
			It("should return true", func() {
				nodePool := opsterv1.NodePool{
					Component: "data",
					Roles:     []string{"data"},
				}
				Expect(helpers.HasDataRole(&nodePool)).To(BeTrue())
			})
		})

		Context("with cluster_manager role only", func() {
			It("should return false", func() {
				nodePool := opsterv1.NodePool{
					Component: "master",
					Roles:     []string{"cluster_manager"},
				}
				Expect(helpers.HasDataRole(&nodePool)).To(BeFalse())
			})
		})

		Context("with multiple roles including data", func() {
			It("should return true", func() {
				nodePool := opsterv1.NodePool{
					Component: "coordinating",
					Roles:     []string{"data", "ingest"},
				}
				Expect(helpers.HasDataRole(&nodePool)).To(BeTrue())
			})
		})

		Context("with no roles", func() {
			It("should return false", func() {
				nodePool := opsterv1.NodePool{
					Component: "coordinating",
					Roles:     []string{},
				}
				Expect(helpers.HasDataRole(&nodePool)).To(BeFalse())
			})
		})
	})

	Describe("Rolling Restart Basic Scenarios", func() {
		Context("when cluster is not initialized", func() {
			BeforeEach(func() {
				instance.Status.Initialized = false
			})

			It("should skip rolling restart", func() {
				result, err := reconciler.Reconcile()
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Requeue).To(BeTrue())
				Expect(result.RequeueAfter).To(Equal(10 * time.Second))
			})
		})

		Context("when upgrade is in progress", func() {
			BeforeEach(func() {
				instance.Status.Version = "2.13.0" // Different from spec version
			})

			It("should skip rolling restart", func() {
				result, err := reconciler.Reconcile()
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())
			})
		})

		Context("when no pending updates exist", func() {
			BeforeEach(func() {
				// Mock StatefulSets with no pending updates
				masterSts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-master",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.StatefulSetStatus{
						ReadyReplicas:    3,
						UpdatedReplicas:  3,
						UpdateRevision:   "rev-1",
						CurrentRevision:  "rev-1",
					},
				}
				dataSts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-data",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.StatefulSetStatus{
						ReadyReplicas:    3,
						UpdatedReplicas:  3,
						UpdateRevision:   "rev-1",
						CurrentRevision:  "rev-1",
					},
				}
				mockClient.EXPECT().GetStatefulSet("test-cluster-master", "default").Return(*masterSts, nil)
				mockClient.EXPECT().GetStatefulSet("test-cluster-data", "default").Return(*dataSts, nil)
			})

			It("should not start rolling restart", func() {
				result, err := reconciler.Reconcile()
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())
			})
		})

		Context("when StatefulSets are not ready", func() {
			BeforeEach(func() {
				// Mock StatefulSets with not ready replicas
				masterSts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-master",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.StatefulSetStatus{
						ReadyReplicas: 2, // Not all ready
					},
				}
				dataSts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster-data",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.StatefulSetStatus{
						ReadyReplicas: 3,
					},
				}
				mockClient.EXPECT().GetStatefulSet("test-cluster-master", "default").Return(*masterSts, nil)
				mockClient.EXPECT().GetStatefulSet("test-cluster-data", "default").Return(*dataSts, nil)
			})

			It("should requeue and wait for readiness", func() {
				result, err := reconciler.Reconcile()
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Requeue).To(BeTrue())
				Expect(result.RequeueAfter).To(Equal(10 * time.Second))
			})
		})
	})

	Describe("Failure Scenarios", func() {
		Context("when StatefulSet retrieval fails", func() {
			BeforeEach(func() {
				mockClient.EXPECT().GetStatefulSet("test-cluster-master", "default").Return(appsv1.StatefulSet{}, fmt.Errorf("API error"))
			})

			It("should return error and stop", func() {
				result, err := reconciler.Reconcile()
				Expect(err).To(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())
			})
		})
	})
})
