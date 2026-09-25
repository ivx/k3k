package syncer_test

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/k3k/k3k-kubelet/controller/syncer"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var ServiceTests = func() {
	var (
		namespace string
		cluster   v1beta1.Cluster
	)

	BeforeEach(func() {
		ctx := context.Background()

		ns := corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ns-"},
		}
		err := hostTestEnv.k8sClient.Create(ctx, &ns)
		Expect(err).NotTo(HaveOccurred())

		namespace = ns.Name

		cluster = v1beta1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cluster-",
				Namespace:    namespace,
			},
		}
		err = hostTestEnv.k8sClient.Create(ctx, &cluster)
		Expect(err).NotTo(HaveOccurred())

		err = syncer.AddServiceSyncer(ctx, virtManager, hostManager, cluster.Name, cluster.Namespace, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ns := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		err := hostTestEnv.k8sClient.Delete(context.Background(), &ns)
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates a service on the host cluster", func() {
		ctx := context.Background()

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "service-",
				Namespace:    "default",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Ports: []corev1.ServicePort{
					{
						Name:       "test-port",
						Port:       8888,
						TargetPort: intstr.FromInt32(8888),
					},
				},
			},
		}

		err := virtTestEnv.k8sClient.Create(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Created service %s in virtual cluster", service.Name))

		var hostService corev1.Service

		hostServiceName := translateName(cluster, service.Namespace, service.Name)

		Eventually(func() error {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			return hostTestEnv.k8sClient.Get(ctx, key, &hostService)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(BeNil())

		By(fmt.Sprintf("Created Service %s in host cluster", hostServiceName))

		Expect(hostService.Spec.Type).To(Equal(corev1.ServiceTypeNodePort))
		Expect(hostService.Spec.Ports[0].Name).To(Equal("test-port"))
		Expect(hostService.Spec.Ports[0].Port).To(Equal(int32(8888)))

		GinkgoWriter.Printf("labels: %v\n", hostService.Labels)
	})

	It("updates a service on the host cluster", func() {
		ctx := context.Background()

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "service-",
				Namespace:    "default",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Ports: []corev1.ServicePort{
					{
						Name:       "test-port",
						Port:       8888,
						TargetPort: intstr.FromInt32(8888),
					},
				},
			},
		}

		err := virtTestEnv.k8sClient.Create(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Created service %s in virtual cluster", service.Name))

		var hostService corev1.Service

		hostServiceName := translateName(cluster, service.Namespace, service.Name)

		Eventually(func() error {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			return hostTestEnv.k8sClient.Get(ctx, key, &hostService)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(BeNil())

		By(fmt.Sprintf("Created Service %s in host cluster", hostServiceName))

		Expect(hostService.Spec.Type).To(Equal(corev1.ServiceTypeNodePort))
		Expect(hostService.Spec.Ports[0].Name).To(Equal("test-port"))
		Expect(hostService.Spec.Ports[0].Port).To(Equal(int32(8888)))

		key := client.ObjectKeyFromObject(service)
		err = virtTestEnv.k8sClient.Get(ctx, key, service)
		Expect(err).NotTo(HaveOccurred())

		service.Spec.Ports[0].Name = "test-port-updated"

		// update virtual service
		err = virtTestEnv.k8sClient.Update(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		// check hostService
		Eventually(func() string {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			err = hostTestEnv.k8sClient.Get(ctx, key, &hostService)
			Expect(err).NotTo(HaveOccurred())

			return hostService.Spec.Ports[0].Name
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(Equal("test-port-updated"))
	})

	It("deletes a service on the host cluster", func() {
		ctx := context.Background()

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "service-",
				Namespace:    "default",
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Ports: []corev1.ServicePort{
					{
						Name:       "test-port",
						Port:       8888,
						TargetPort: intstr.FromInt32(8888),
					},
				},
			},
		}

		err := virtTestEnv.k8sClient.Create(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Created service %s in virtual cluster", service.Name))

		var hostService corev1.Service

		hostServiceName := translateName(cluster, service.Namespace, service.Name)

		Eventually(func() error {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			return hostTestEnv.k8sClient.Get(ctx, key, &hostService)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(BeNil())

		By(fmt.Sprintf("Created service %s in host cluster", hostServiceName))

		Expect(hostService.Spec.Type).To(Equal(corev1.ServiceTypeNodePort))
		Expect(hostService.Spec.Ports[0].Name).To(Equal("test-port"))
		Expect(hostService.Spec.Ports[0].Port).To(Equal(int32(8888)))

		err = virtTestEnv.k8sClient.Delete(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			err := hostTestEnv.k8sClient.Get(ctx, key, &hostService)

			return apierrors.IsNotFound(err)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(BeTrue())
	})

	It("will not create a service on the host cluster if disabled", func() {
		ctx := context.Background()

		cluster.Spec.Sync.Services.Enabled = false
		err := hostTestEnv.k8sClient.Update(ctx, &cluster)
		Expect(err).NotTo(HaveOccurred())

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "service-",
				Namespace:    "default",
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Ports: []corev1.ServicePort{
					{
						Name:       "test-port",
						Port:       8888,
						TargetPort: intstr.FromInt32(8888),
					},
				},
			},
		}

		err = virtTestEnv.k8sClient.Create(ctx, service)
		Expect(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Created service %s in virtual cluster", service.Name))

		var hostService corev1.Service

		hostServiceName := translateName(cluster, service.Namespace, service.Name)

		Eventually(func() bool {
			key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}
			err = hostTestEnv.k8sClient.Get(ctx, key, &hostService)

			return apierrors.IsNotFound(err)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(BeTrue())
	})

	It("gives a virtual service a new ClusterIP when the host already uses it", func() {
		ctx := context.Background()

		// a host service holds an IP; the virtual service asks for the same IP
		blocker := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "blocker-", Namespace: namespace},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "p", Port: 80}}},
		}
		Expect(hostTestEnv.k8sClient.Create(ctx, blocker)).To(Succeed())

		takenIP := blocker.Spec.ClusterIP
		Expect(takenIP).NotTo(BeEmpty())

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "service-", Namespace: "default"},
			Spec: corev1.ServiceSpec{
				ClusterIP: takenIP,
				Ports:     []corev1.ServicePort{{Name: "p", Port: 80}},
			},
		}
		Expect(virtTestEnv.k8sClient.Create(ctx, service)).To(Succeed())

		By(fmt.Sprintf("Created service %s with the host's IP %s in the virtual cluster", service.Name, takenIP))

		hostServiceName := translateName(cluster, service.Namespace, service.Name)

		var virtService, hostService corev1.Service

		// the syncer recreates the virtual service without the taken IP, the
		// virtual cluster assigns a new one, and the host copy carries it
		Eventually(func(g Gomega) {
			g.Expect(virtTestEnv.k8sClient.Get(ctx, client.ObjectKey{Name: service.Name, Namespace: "default"}, &virtService)).To(Succeed())
			g.Expect(virtService.Spec.ClusterIP).NotTo(Equal(takenIP))
			g.Expect(hostTestEnv.k8sClient.Get(ctx, client.ObjectKey{Name: hostServiceName, Namespace: namespace}, &hostService)).To(Succeed())
			g.Expect(hostService.Spec.ClusterIP).To(Equal(virtService.Spec.ClusterIP))
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 20).
			Should(Succeed())

		Expect(virtService.UID).NotTo(Equal(service.UID), "the virtual service was created again")
	})

	It("creates the host copy again when it is deleted on the host", func() {
		ctx := context.Background()

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "service-", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "p", Port: 80}}},
		}
		Expect(virtTestEnv.k8sClient.Create(ctx, service)).To(Succeed())

		hostServiceName := translateName(cluster, service.Namespace, service.Name)
		key := client.ObjectKey{Name: hostServiceName, Namespace: namespace}

		var hostService corev1.Service

		Eventually(func() error {
			return hostTestEnv.k8sClient.Get(ctx, key, &hostService)
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(Succeed())

		oldUID := hostService.UID
		Expect(hostTestEnv.k8sClient.Delete(ctx, &hostService)).To(Succeed())

		// no change on the virtual side: only the host watch can bring it back
		Eventually(func(g Gomega) {
			var again corev1.Service
			g.Expect(hostTestEnv.k8sClient.Get(ctx, key, &again)).To(Succeed())
			g.Expect(again.UID).NotTo(Equal(oldUID))
			g.Expect(again.Spec.ClusterIP).To(Equal(service.Spec.ClusterIP))
		}).
			WithPolling(time.Millisecond * 300).
			WithTimeout(time.Second * 10).
			Should(Succeed())
	})
}
