package app

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SAP/crossplane-provider-cloudfoundry/apis/resources/v1alpha1"
	pcv1beta1 "github.com/SAP/crossplane-provider-cloudfoundry/apis/v1beta1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func backendFixture(t *testing.T) (*k8sExternal, *v1alpha1.App) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{appsv1.AddToScheme, corev1.AddToScheme, v1alpha1.AddToScheme, pcv1beta1.SchemeBuilder.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cr := &v1alpha1.App{ObjectMeta: metav1.ObjectMeta{Name: "managed-test", UID: types.UID("00000000-0000-0000-0000-000000000001")}, Spec: v1alpha1.AppSpec{ForProvider: v1alpha1.AppParameters{
		Name: "test-app", Lifecycle: lifecycleDocker, SpaceReference: v1alpha1.SpaceReference{SpaceName: ptr.To("target-space")},
		Docker:      &v1alpha1.DockerConfiguration{Image: "crccheck/hello-world"},
		Processes:   []v1alpha1.ProcessConfiguration{{Type: ptr.To(webProcessType), Instances: ptr.To(uint(1))}},
		Environment: map[string]string{"PORT": "8000", "REMOVABLE": "original"},
	}}}
	return &k8sExternal{target: fake.NewClientBuilder().WithScheme(scheme).WithGlobalResourceVersionCounter().Build(), kube: fake.NewClientBuilder().WithScheme(scheme).Build()}, cr
}

func getWorkload(t *testing.T, b *k8sExternal, cr *v1alpha1.App) (*appsv1.Deployment, *corev1.Service) {
	t.Helper()
	dep, svc := &appsv1.Deployment{}, &corev1.Service{}
	key, err := b.identity(context.Background(), cr, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range []k8s.Object{dep, svc} {
		if err := b.target.Get(context.Background(), key, obj); err != nil {
			t.Fatal(err)
		}
	}
	return dep, svc
}

func assertObservation(t *testing.T, b *k8sExternal, cr *v1alpha1.App, exists, current bool) {
	t.Helper()
	got, err := b.Observe(context.Background(), cr)
	if err != nil || got.ResourceExists != exists || got.ResourceUpToDate != current {
		t.Fatalf("Observe = %+v, %v; want exists=%v current=%v", got, err, exists, current)
	}
}

func TestKubernetesBackendLifecycle(t *testing.T) {
	b, cr := backendFixture(t)
	assertObservation(t, b, cr, false, false)
	if meta.GetExternalName(cr) != "target-space/test-app" {
		t.Fatal("external name not initialized")
	}
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	dep, svc := getWorkload(t, b, cr)
	if appContainer(dep).Ports[0].ContainerPort != 8000 || svc.Spec.Ports[0].Port != 8000 || svc.Spec.Ports[0].TargetPort.IntVal != 8000 {
		t.Fatal("PORT8000 not propagated")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatal("not a ClusterIP Service")
	}
	if _, err := b.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []k8s.Object{&appsv1.Deployment{}, &corev1.Service{}} {
		if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: workloadNamespace(cr), Name: workloadName(cr)}, obj); !apierrors.IsNotFound(err) {
			t.Fatalf("delete %T: %v", obj, err)
		}
	}
}

func TestKubernetesBackendIndependentDrift(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *k8sExternal, *v1alpha1.App)
	}{
		{"image", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			cr.Spec.ForProvider.Docker.Image = "example.com/changed:latest"
		}},
		{"replicas", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			cr.Spec.ForProvider.Processes[0].Instances = ptr.To(uint(2))
		}},
		{"env", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			cr.Spec.ForProvider.Environment["REMOVABLE"] = "changed"
		}},
		{"removed env", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			delete(cr.Spec.ForProvider.Environment, "REMOVABLE")
		}},
		{"changed port", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) { cr.Spec.ForProvider.Environment["PORT"] = "9000" }},
		{"missing Service", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			_, svc := getWorkload(t, b, cr)
			if err := b.target.Delete(context.Background(), svc); err != nil {
				t.Fatal(err)
			}
		}},
		{"service drift", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			_, svc := getWorkload(t, b, cr)
			svc.Spec.Ports[0].Port = 1234
			svc.Spec.Ports[0].TargetPort.IntVal = 4321
			svc.Spec.Selector = map[string]string{"wrong": "selector"}
			if err := b.target.Update(context.Background(), svc); err != nil {
				t.Fatal(err)
			}
		}},
		{"container port drift", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			dep, _ := getWorkload(t, b, cr)
			appContainer(dep).Ports[0].ContainerPort = 1234
			if err := b.target.Update(context.Background(), dep); err != nil {
				t.Fatal(err)
			}
		}},
		{"legacy annotation", func(t *testing.T, b *k8sExternal, cr *v1alpha1.App) {
			dep, _ := getWorkload(t, b, cr)
			delete(dep.Spec.Template.Annotations, ownedEnvAnnotation)
			if err := b.target.Update(context.Background(), dep); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, cr := backendFixture(t)
			if _, err := b.Create(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			dep, svc := getWorkload(t, b, cr)
			appContainer(dep).Env = append(appContainer(dep).Env, corev1.EnvVar{Name: "INJECTED", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}})
			dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar", Image: "example.com/sidecar"})
			if err := b.target.Update(context.Background(), dep); err != nil {
				t.Fatal(err)
			}
			svc.Spec.ClusterIP = "10.0.0.1"
			svc.Spec.SessionAffinity = corev1.ServiceAffinityClientIP
			if err := b.target.Update(context.Background(), svc); err != nil {
				t.Fatal(err)
			}
			assertObservation(t, b, cr, true, true)
			tc.mutate(t, b, cr)
			assertObservation(t, b, cr, true, false)
			if _, err := b.Update(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			assertObservation(t, b, cr, true, true)
			dep, svc = getWorkload(t, b, cr)
			if len(dep.Spec.Template.Spec.Containers) != 2 || dep.Spec.Template.Spec.Containers[1].Name != "sidecar" {
				t.Fatal("sidecar lost")
			}
			injected, removed := 0, false
			for _, env := range appContainer(dep).Env {
				if env.Name == "INJECTED" && env.ValueFrom != nil {
					injected++
				}
				if env.Name == "REMOVABLE" {
					removed = true
				}
			}
			if injected != 1 {
				t.Fatal("injected env lost or duplicated")
			}
			if tc.name == "removed env" && removed {
				t.Fatal("removed owned env retained")
			}
			if tc.name != "missing Service" && (svc.Spec.ClusterIP != "10.0.0.1" || svc.Spec.SessionAffinity != corev1.ServiceAffinityClientIP) {
				t.Fatal("Service defaults/ClusterIP lost")
			}
			if tc.name == "changed port" && (appContainer(dep).Ports[0].ContainerPort != 9000 || svc.Spec.Ports[0].Port != 9000 || svc.Spec.Ports[0].TargetPort.IntVal != 9000) {
				t.Fatal("changed PORT not healed")
			}
			if dep.Spec.Template.Annotations[ownedEnvAnnotation] == "" {
				t.Fatal("ownership not advanced")
			}
		})
	}
}

type writeCountingClient struct {
	k8s.Client
	writes int
}

func (c *writeCountingClient) Create(ctx context.Context, obj k8s.Object, opts ...k8s.CreateOption) error {
	c.writes++
	return c.Client.Create(ctx, obj, opts...)
}
func (c *writeCountingClient) Update(ctx context.Context, obj k8s.Object, opts ...k8s.UpdateOption) error {
	c.writes++
	return c.Client.Update(ctx, obj, opts...)
}

func TestKubernetesBackendInvalidPortNoWrites(t *testing.T) {
	for _, port := range []string{"", "0", "65536", "-1", "abc", "8000.0", "+8000"} {
		t.Run(port, func(t *testing.T) {
			b, cr := backendFixture(t)
			counter := &writeCountingClient{Client: b.target}
			b.target = counter
			cr.Spec.ForProvider.Environment["PORT"] = port
			if _, err := b.Observe(context.Background(), cr); err == nil {
				t.Fatal("Observe accepted invalid PORT")
			}
			if _, err := b.Create(context.Background(), cr); err == nil {
				t.Fatal("Create accepted invalid PORT")
			}
			if _, err := b.Update(context.Background(), cr); err == nil {
				t.Fatal("Update accepted invalid PORT")
			}
			if counter.writes != 0 {
				t.Fatalf("invalid PORT caused %d writes", counter.writes)
			}
		})
	}
}

func TestKubernetesBackendValidationAndNamespace(t *testing.T) {
	b, cr := backendFixture(t)
	delete(cr.Spec.ForProvider.Environment, "PORT")
	port, err := desiredPort(cr)
	if err != nil || port != 8080 {
		t.Fatalf("fallback port %d: %v", port, err)
	}
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	getWorkload(t, b, cr)
	cr.Spec.ForProvider.Lifecycle = "buildpack"
	if _, err := b.Create(context.Background(), cr); err == nil {
		t.Fatal("buildpack accepted")
	}
}

func TestKubernetesBackendOwnershipMigration(t *testing.T) {
	b, cr := backendFixture(t)
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	dep, _ := getWorkload(t, b, cr)
	if got := dep.Spec.Template.Annotations[ownedEnvAnnotation]; got != `["PORT","REMOVABLE"]` {
		t.Fatalf("owned key names = %s", got)
	}
	delete(dep.Spec.Template.Annotations, ownedEnvAnnotation)
	appContainer(dep).Env = append(appContainer(dep).Env, corev1.EnvVar{Name: "LEGACY_UNKNOWN", Value: "preserve"})
	if err := b.target.Update(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	delete(cr.Spec.ForProvider.Environment, "REMOVABLE")
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	dep, _ = getWorkload(t, b, cr)
	unknown := false
	for _, variable := range appContainer(dep).Env {
		if variable.Name == "REMOVABLE" {
			t.Fatal("adopted key not removed")
		}
		if variable.Name == "LEGACY_UNKNOWN" && variable.Value == "preserve" {
			unknown = true
		}
	}
	if !unknown {
		t.Fatal("legacy unknown key incorrectly adopted")
	}
	if got := dep.Spec.Template.Annotations[ownedEnvAnnotation]; got != `["PORT"]` {
		t.Fatalf("ownership not converged: %s", got)
	}
}

func TestKubernetesBackendPortBoundaries(t *testing.T) {
	for _, port := range []string{"1", "65535"} {
		b, cr := backendFixture(t)
		cr.Spec.ForProvider.Environment["PORT"] = port
		if _, err := b.Create(context.Background(), cr); err != nil {
			t.Fatal(err)
		}
		assertObservation(t, b, cr, true, true)
	}
}

func (c *writeCountingClient) Delete(ctx context.Context, obj k8s.Object, opts ...k8s.DeleteOption) error {
	c.writes++
	return c.Client.Delete(ctx, obj, opts...)
}

func TestKubernetesOwnershipCollisions(t *testing.T) {
	for _, kind := range []string{"deployment", "service", "secret", "other app", "missing uid"} {
		t.Run(kind, func(t *testing.T) {
			b, cr := backendFixture(t)
			if kind == "secret" {
				b, cr, _ = registryFixture(t)
			}
			if kind == "other app" {
				if _, err := b.Create(context.Background(), cr); err != nil {
					t.Fatal(err)
				}
				cr = cr.DeepCopy()
				cr.UID = "different-app-uid"
			} else if kind == "missing uid" {
				cr.UID = ""
			} else {
				var obj k8s.Object
				switch kind {
				case "deployment":
					obj = b.buildDeployment(cr)
				case "service":
					obj = b.buildService(cr)
				case "secret":
					obj = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: workloadNamespace(cr), Name: registrySecretName(cr)}}
				}
				obj.SetAnnotations(nil)
				if err := b.target.Create(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
			}
			counter := &writeCountingClient{Client: b.target}
			b.target = counter
			if _, err := b.Observe(context.Background(), cr); err == nil {
				t.Fatal("Observe accepted collision")
			}
			if _, err := b.Create(context.Background(), cr); err == nil {
				t.Fatal("Create accepted collision")
			}
			if _, err := b.Update(context.Background(), cr); err == nil {
				t.Fatal("Update accepted collision")
			}
			_, deleteErr := b.Delete(context.Background(), cr)
			if kind == "missing uid" && deleteErr == nil {
				t.Fatal("Delete accepted missing UID")
			}
			if kind != "missing uid" && deleteErr != nil {
				t.Fatalf("foreign collision blocked teardown: %v", deleteErr)
			}
			if counter.writes != 0 {
				t.Fatalf("collision caused %d writes", counter.writes)
			}
		})
	}
}

func TestKubernetesPinnedIdentity(t *testing.T) {
	for _, mutate := range []string{"name", "namespace", "removed fields", "tampered", "invalid"} {
		t.Run(mutate, func(t *testing.T) {
			b, cr := backendFixture(t)
			if _, err := b.Create(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			original := meta.GetExternalName(cr)
			switch mutate {
			case "name":
				cr.Spec.ForProvider.Name = "renamed"
			case "namespace":
				cr.Spec.ForProvider.SpaceName = ptr.To("changed")
			case "removed fields":
				cr.Spec.ForProvider.SpaceName = nil
				cr.Spec.ForProvider.Name = ""
			case "invalid":
				meta.SetExternalName(cr, "bad/extra/path")
			case "tampered":
				obj := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "foreign", Name: "foreign"}}
				if err := b.target.Create(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
				meta.SetExternalName(cr, "foreign/foreign")
				cr.Spec.ForProvider.SpaceName = ptr.To("foreign")
				cr.Spec.ForProvider.Name = "foreign"
			}
			counter := &writeCountingClient{Client: b.target}
			b.target = counter
			if _, err := b.Observe(context.Background(), cr); err == nil {
				t.Fatal("Observe accepted changed identity")
			}
			if _, err := b.Create(context.Background(), cr); err == nil {
				t.Fatal("Create accepted changed identity")
			}
			if _, err := b.Update(context.Background(), cr); err == nil {
				t.Fatal("Update accepted changed identity")
			}
			if counter.writes != 0 {
				t.Fatal("identity mutation wrote target resources")
			}
			if mutate == "invalid" {
				if _, err := b.Delete(context.Background(), cr); err == nil {
					t.Fatal("invalid backend identity accepted")
				}
				return
			}
			if mutate == "tampered" {
				if _, err := b.Delete(context.Background(), cr); err != nil {
					t.Fatal(err)
				}
				if counter.writes != 0 {
					t.Fatal("tampered identity mutated foreign target")
				}
				if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "foreign", Name: "foreign"}, &appsv1.Deployment{}); err != nil {
					t.Fatal("foreign target was deleted")
				}
				return
			}
			if meta.GetExternalName(cr) != original {
				t.Fatal("external-name changed")
			}
			if _, err := b.Delete(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			for _, obj := range []k8s.Object{&appsv1.Deployment{}, &corev1.Service{}} {
				if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "target-space", Name: "test-app"}, obj); !apierrors.IsNotFound(err) {
					t.Fatalf("original not deleted: %v", err)
				}
			}
		})
	}
}

func TestKubernetesSpaceMapping(t *testing.T) {
	for _, mode := range []string{"ref", "selector", "guid", "ambiguous", "missing", "controller", "controller mismatch"} {
		t.Run(mode, func(t *testing.T) {
			b, cr := backendFixture(t)
			space := &v1alpha1.Space{ObjectMeta: metav1.ObjectMeta{Name: "managed-space", Labels: map[string]string{"purpose": "test"}}, Spec: v1alpha1.SpaceSpec{ForProvider: v1alpha1.SpaceParameters{Name: "actual-namespace"}}, Status: v1alpha1.SpaceStatus{AtProvider: v1alpha1.SpaceObservation{ID: "cf-space-guid"}}}
			if err := b.kube.Create(context.Background(), space); err != nil {
				t.Fatal(err)
			}
			cr.Spec.ForProvider.SpaceName = nil
			cr.Spec.ForProvider.Space = ptr.To("cf-space-guid")
			switch mode {
			case "ref":
				cr.Spec.ForProvider.SpaceRef = &xpv2.Reference{Name: space.Name}
			case "missing":
				cr.Spec.ForProvider.Space = ptr.To("unknown-guid")
			case "selector", "ambiguous", "controller", "controller mismatch":
				cr.Spec.ForProvider.SpaceSelector = &xpv2.Selector{MatchLabels: map[string]string{"purpose": "test"}}
				if mode == "ambiguous" {
					other := space.DeepCopy()
					other.Name = "second"
					other.ResourceVersion = ""
					if err := b.kube.Create(context.Background(), other); err != nil {
						t.Fatal(err)
					}
				}
				if strings.HasPrefix(mode, "controller") {
					owner := metav1.OwnerReference{APIVersion: "test/v1", Kind: "Owner", Name: "owner", UID: "shared-owner", Controller: ptr.To(true)}
					cr.OwnerReferences = []metav1.OwnerReference{owner}
					space.OwnerReferences = []metav1.OwnerReference{owner}
					if mode == "controller mismatch" {
						space.OwnerReferences[0].UID = "other-owner"
					}
					if err := b.kube.Update(context.Background(), space); err != nil {
						t.Fatal(err)
					}
					cr.Spec.ForProvider.SpaceSelector.MatchControllerRef = ptr.To(true)
				}
			}
			if mode == "ambiguous" || mode == "missing" || mode == "controller mismatch" {
				if _, err := b.Create(context.Background(), cr); err == nil {
					t.Fatal("ambiguous/unmapped Space accepted")
				}
				return
			}
			if _, err := b.Create(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			if meta.GetExternalName(cr) != "actual-namespace/test-app" || cr.Spec.ForProvider.SpaceName != nil {
				t.Fatal("Space GUID used as namespace or dual reference mutated")
			}
			getWorkload(t, b, cr)
			if err := b.kube.Delete(context.Background(), space); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Delete(context.Background(), cr); err != nil {
				t.Fatalf("pinned deletion depended on Space: %v", err)
			}
		})
	}
}

func registryFixture(t *testing.T) (*k8sExternal, *v1alpha1.App, *corev1.Secret) {
	t.Helper()
	b, cr := backendFixture(t)
	source := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "controlplane", Name: "registry"}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.example.com":{"auth":"dXNlcjpwYXNz"}}}`)}}
	if err := b.kube.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	cr.Spec.ForProvider.Docker.Credentials = &xpv2.SecretReference{Namespace: source.Namespace, Name: source.Name}
	return b, cr, source
}

func TestKubernetesRegistryCredentials(t *testing.T) {
	b, cr, source := registryFixture(t)
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	key := types.NamespacedName{Namespace: "target-space", Name: registrySecretName(cr)}
	copied := &corev1.Secret{}
	if err := b.target.Get(context.Background(), key, copied); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(copied.Data[corev1.DockerConfigJsonKey], source.Data[corev1.DockerConfigJsonKey]) || copied.Type != corev1.SecretTypeDockerConfigJson || checkOwner(cr, copied) != nil {
		t.Fatal("credentials not safely copied/owned")
	}
	dep, _ := getWorkload(t, b, cr)
	if len(dep.Spec.Template.Spec.ImagePullSecrets) != 1 || dep.Spec.Template.Spec.ImagePullSecrets[0].Name != key.Name || len(key.Name) > 63 {
		t.Fatal("pull secret not wired safely")
	}
	source.Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{"registry.example.com":{"auth":"dXNlcjpyb3RhdGVk"}}}`)
	if err := b.kube.Update(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	if err := b.target.Get(context.Background(), key, copied); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(copied.Data[corev1.DockerConfigJsonKey], source.Data[corev1.DockerConfigJsonKey]) {
		t.Fatal("rotation not propagated")
	}
	if err := b.target.Delete(context.Background(), copied); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	dep, _ = getWorkload(t, b, cr)
	dep.Spec.Template.Spec.ImagePullSecrets = nil
	if err := b.target.Update(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	cr.Spec.ForProvider.Docker.Credentials = nil
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	if err := b.target.Get(context.Background(), key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatal("removed credentials Secret retained")
	}
	dep, _ = getWorkload(t, b, cr)
	if len(dep.Spec.Template.Spec.ImagePullSecrets) != 0 {
		t.Fatal("removed pullref retained")
	}
	cr.Spec.ForProvider.Docker.Credentials = &xpv2.SecretReference{Namespace: source.Namespace, Name: source.Name}
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if err := b.target.Get(context.Background(), key, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatal("Delete retained registry Secret")
	}
}

func TestKubernetesInvalidRegistryNoWrites(t *testing.T) {
	for _, invalid := range []string{"missing", "malformed", "invalid auth"} {
		t.Run(invalid, func(t *testing.T) {
			b, cr, source := registryFixture(t)
			if invalid == "missing" {
				if err := b.kube.Delete(context.Background(), source); err != nil {
					t.Fatal(err)
				}
			} else {
				source.Data[corev1.DockerConfigJsonKey] = []byte(`not-json`)
				if invalid == "invalid auth" {
					source.Data[corev1.DockerConfigJsonKey] = []byte(`{"auths":{"registry.example.com":{"auth":"invalid-base64"}}}`)
				}
				if err := b.kube.Update(context.Background(), source); err != nil {
					t.Fatal(err)
				}
			}
			counter := &writeCountingClient{Client: b.target}
			b.target = counter
			if _, err := b.Create(context.Background(), cr); err == nil {
				t.Fatal("bad registry accepted")
			}
			if _, err := b.Update(context.Background(), cr); err == nil {
				t.Fatal("bad registry accepted")
			}
			if counter.writes != 0 {
				t.Fatal("bad registry caused writes")
			}
		})
	}
}

func TestKubernetesRolloutStatus(t *testing.T) {
	for _, state := range []string{"stale", "partial", "old replicas", "complete", "zero"} {
		t.Run(state, func(t *testing.T) {
			b, cr := backendFixture(t)
			dep := b.buildDeployment(cr)
			dep.UID = "real-deployment-uid"
			dep.Generation = 2
			dep.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1, Replicas: 1}
			switch state {
			case "stale":
				dep.Status.ObservedGeneration = 1
			case "partial":
				dep.Status.UpdatedReplicas = 0
			case "old replicas":
				dep.Status.Replicas = 2
			case "zero":
				cr.Spec.ForProvider.Processes[0].Instances = ptr.To(uint(0))
				dep.Spec.Replicas = ptr.To(int32(0))
			}
			for _, obj := range []k8s.Object{dep, b.buildService(cr)} {
				if err := b.target.Create(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
			}
			assertObservation(t, b, cr, true, true)
			want := "STOPPED"
			if state == "complete" {
				want = "STARTED"
			}
			if cr.Status.AtProvider.State != want || cr.Status.AtProvider.GUID != string(dep.UID) {
				t.Fatalf("state=%s GUID=%s", cr.Status.AtProvider.State, cr.Status.AtProvider.GUID)
			}
			if (cr.GetCondition(xpv2.TypeReady).Status == corev1.ConditionTrue) != (state == "complete") {
				t.Fatal("Available condition incorrect")
			}
		})
	}
	b, cr := backendFixture(t)
	dep := b.buildDeployment(cr)
	dep.UID = "observed-uid"
	if err := b.target.Create(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, false)
	if cr.Status.AtProvider.GUID != "observed-uid" {
		t.Fatal("missing Service prevented status refresh")
	}
}

func TestKubernetesShellCommandHealing(t *testing.T) {
	b, cr := backendFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Create(ctx, cr); err != nil {
		t.Fatal(err)
	}
	command := `printf '%s' 'quoted value with spaces and "double quotes"'`
	cr.Spec.ForProvider.Processes[0].Command = &command
	assertObservation(t, b, cr, true, false)
	if _, err := b.Update(ctx, cr); err != nil {
		t.Fatal(err)
	}
	dep, _ := getWorkload(t, b, cr)
	argv := appContainer(dep).Command
	if len(argv) == 0 {
		t.Fatal("updated workload has no command")
	}
	output, err := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("workload command failed: %v; output %q", err, output)
	}
	if got, want := string(output), `quoted value with spaces and "double quotes"`; got != want {
		t.Fatalf("workload command output = %q, want %q", got, want)
	}
	assertObservation(t, b, cr, true, true)
}

func TestKubernetesKubeconfigSafety(t *testing.T) {
	for _, unsafe := range []string{"safe", "exec", "auth provider", "token file", "cert file", "key file", "ca file", "missing context", "missing user", "missing cluster", "filesystem source"} {
		t.Run(unsafe, func(t *testing.T) {
			b, cr := backendFixture(t)
			config := clientcmdapi.NewConfig()
			config.CurrentContext = "target"
			config.Contexts["target"] = &clientcmdapi.Context{Cluster: "target", AuthInfo: "user"}
			config.Clusters["target"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:1", InsecureSkipTLSVerify: true}
			config.AuthInfos["user"] = &clientcmdapi.AuthInfo{Token: "inline-token"}
			auth := config.AuthInfos["user"]
			switch unsafe {
			case "exec":
				auth.Exec = &clientcmdapi.ExecConfig{Command: "forbidden"}
			case "auth provider":
				auth.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "forbidden"}
			case "token file":
				auth.TokenFile = "/forbidden"
			case "cert file":
				auth.ClientCertificate = "/forbidden"
			case "key file":
				auth.ClientKey = "/forbidden"
			case "ca file":
				config.Clusters["target"].CertificateAuthority = "/forbidden"
			case "missing context":
				config.CurrentContext = "missing"
			case "missing user":
				config.Contexts["target"].AuthInfo = "missing"
			case "missing cluster":
				config.Contexts["target"].Cluster = "missing"
			}
			data, err := clientcmd.Write(*config)
			if err != nil {
				t.Fatal(err)
			}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "cp", Name: "kubeconfig"}, Data: map[string][]byte{"config": data}}
			pc := &pcv1beta1.ProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "target"}, Spec: pcv1beta1.ProviderConfigSpec{Credentials: pcv1beta1.ProviderCredentials{Source: xpv2.CredentialsSourceSecret, CommonCredentialSelectors: xpv2.CommonCredentialSelectors{SecretRef: &xpv2.SecretKeySelector{SecretReference: xpv2.SecretReference{Namespace: "cp", Name: "kubeconfig"}, Key: "config"}}}}}
			if unsafe == "filesystem source" {
				pc.Spec.Credentials.Source = xpv2.CredentialsSourceFilesystem
			}
			for _, obj := range []k8s.Object{secret, pc} {
				if err := b.kube.Create(context.Background(), obj); err != nil {
					t.Fatal(err)
				}
			}
			cr.SetProviderConfigReference(&xpv2.Reference{Name: pc.Name})
			cl, rt, err := newKubeBackendClient(context.Background(), b.kube, cr)
			if unsafe == "safe" {
				if err != nil || cl == nil {
					t.Fatalf("safe inline rejected: %v", err)
				}
				if err := (&k8sExternal{rt: rt}).Disconnect(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || cl != nil {
				t.Fatal("unsafe configuration built target client")
			}
		})
	}
}

type recreateBeforeDeleteClient struct {
	k8s.Client
	t         *testing.T
	recreated bool
}

func (c *recreateBeforeDeleteClient) Delete(ctx context.Context, obj k8s.Object, options ...k8s.DeleteOption) error {
	if !c.recreated {
		c.recreated = true
		resolved := &k8s.DeleteOptions{}
		for _, option := range options {
			option.ApplyToDelete(resolved)
		}
		if resolved.Preconditions == nil || resolved.Preconditions.UID == nil || *resolved.Preconditions.UID != obj.GetUID() {
			c.t.Fatal("delete lacks matching target UID precondition")
		}
		if err := c.Client.Delete(ctx, obj); err != nil {
			return err
		}
		replacement := obj.DeepCopyObject().(k8s.Object)
		replacement.SetResourceVersion("")
		replacement.SetUID("foreign-recreated-uid")
		replacement.SetAnnotations(nil)
		if err := c.Client.Create(ctx, replacement); err != nil {
			return err
		}
	}
	return c.Client.Delete(ctx, obj, options...)
}

func TestKubernetesDeleteRecreationRace(t *testing.T) {
	b, cr := backendFixture(t)
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	dep, svc := getWorkload(t, b, cr)
	dep.UID, svc.UID = "deployment-uid", "service-uid"
	for _, obj := range []k8s.Object{dep, svc} {
		if err := b.target.Update(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	b.target = &recreateBeforeDeleteClient{Client: b.target, t: t}
	if _, err := b.Delete(context.Background(), cr); !apierrors.IsConflict(err) {
		t.Fatalf("expected recreation precondition conflict, got %v", err)
	}
	remaining := &corev1.Service{}
	if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "target-space", Name: "test-app"}, remaining); err != nil {
		t.Fatal(err)
	}
	if remaining.UID != "foreign-recreated-uid" {
		t.Fatal("recreated foreign resource deleted")
	}
}

func TestKubernetesLegacyCFIdentityDenied(t *testing.T) {
	b, cr := backendFixture(t)
	meta.SetExternalName(cr, "cf-app-guid")
	counter := &writeCountingClient{Client: b.target}
	b.target = counter
	if _, err := b.Delete(context.Background(), cr); err == nil || !strings.Contains(err.Error(), "backend mismatch") {
		t.Fatalf("legacy CF delete: %v", err)
	}
	if _, err := b.Create(context.Background(), cr); err == nil {
		t.Fatal("legacy CF external-name adopted")
	}
	if counter.writes != 0 || meta.GetExternalName(cr) != "cf-app-guid" {
		t.Fatal("legacy CF identity touched")
	}
}

type forbidSecretReadsClient struct {
	k8s.Client
	secretReads int
}

func (c *forbidSecretReadsClient) Get(ctx context.Context, key k8s.ObjectKey, obj k8s.Object, options ...k8s.GetOption) error {
	if _, secret := obj.(*corev1.Secret); secret {
		c.secretReads++
		return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, key.Name, errors.New("target Secret reads forbidden"))
	}
	return c.Client.Get(ctx, key, obj, options...)
}

func TestKubernetesPublicAppWithoutSecretRBAC(t *testing.T) {
	b, cr := backendFixture(t)
	limited := &forbidSecretReadsClient{Client: b.target}
	b.target = limited
	assertObservation(t, b, cr, false, false)
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, true, true)
	cr.Spec.ForProvider.Environment["REMOVABLE"] = "changed"
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	cr.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(1, 0)))
	assertObservation(t, b, cr, true, false)
	if _, err := b.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, b, cr, false, false)
	if limited.secretReads != 0 {
		t.Fatalf("public App attempted %d Secret reads", limited.secretReads)
	}
}

func TestKubernetesRemovedCredentialsStillDiscovered(t *testing.T) {
	b, cr, _ := registryFixture(t)
	if _, err := b.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	cr.Spec.ForProvider.Docker.Credentials = nil
	original := b.target
	limited := &forbidSecretReadsClient{Client: original}
	b.target = limited
	if _, err := b.Update(context.Background(), cr); !apierrors.IsForbidden(err) {
		t.Fatalf("must discover live formerly-owned pull Secret: %v", err)
	}
	if limited.secretReads == 0 {
		t.Fatal("formerly-owned Secret silently skipped")
	}
	b.target = original
	if _, err := b.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "target-space", Name: registrySecretName(cr)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatal("formerly-owned Secret orphaned")
	}
	b.target = limited
	before := limited.secretReads
	assertObservation(t, b, cr, true, true)
	if limited.secretReads != before {
		t.Fatal("public mode still requires Secret reads after cleanup")
	}
}

func TestKubernetesDeletionObservationIgnoresGoneDependencies(t *testing.T) {
	for _, mutation := range []string{"space gone", "selector ambiguous", "source registry gone", "invalid image", "invalid port", "removed fields", "deployment already gone"} {
		t.Run(mutation, func(t *testing.T) {
			b, cr, source := registryFixture(t)
			space := &v1alpha1.Space{ObjectMeta: metav1.ObjectMeta{Name: "teardown-space", Labels: map[string]string{"purpose": "teardown"}}, Spec: v1alpha1.SpaceSpec{ForProvider: v1alpha1.SpaceParameters{Name: "target-space"}}}
			if err := b.kube.Create(context.Background(), space); err != nil {
				t.Fatal(err)
			}
			cr.Spec.ForProvider.SpaceName = nil
			cr.Spec.ForProvider.SpaceRef = &xpv2.Reference{Name: space.Name}
			if _, err := b.Create(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "space gone":
				if err := b.kube.Delete(context.Background(), space); err != nil {
					t.Fatal(err)
				}
			case "selector ambiguous":
				cr.Spec.ForProvider.SpaceRef = nil
				cr.Spec.ForProvider.SpaceSelector = &xpv2.Selector{MatchLabels: space.Labels}
				other := space.DeepCopy()
				other.Name = "another"
				other.ResourceVersion = ""
				if err := b.kube.Create(context.Background(), other); err != nil {
					t.Fatal(err)
				}
			case "source registry gone":
				if err := b.kube.Delete(context.Background(), source); err != nil {
					t.Fatal(err)
				}
			case "invalid image":
				cr.Spec.ForProvider.Docker.Image = ""
			case "invalid port":
				cr.Spec.ForProvider.Environment["PORT"] = "invalid"
			case "removed fields":
				cr.Spec.ForProvider.Docker = nil
				cr.Spec.ForProvider.SpaceRef = nil
				cr.Spec.ForProvider.Name = ""
			case "deployment already gone":
				dep, _ := getWorkload(t, b, cr)
				if err := b.target.Delete(context.Background(), dep); err != nil {
					t.Fatal(err)
				}
			}
			cr.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(1, 0)))
			assertObservation(t, b, cr, true, false)
			if _, err := b.Delete(context.Background(), cr); err != nil {
				t.Fatalf("pinned teardown: %v", err)
			}
			assertObservation(t, b, cr, false, false)
			for _, obj := range []k8s.Object{&appsv1.Deployment{}, &corev1.Service{}, &corev1.Secret{}} {
				name := "test-app"
				if _, secret := obj.(*corev1.Secret); secret {
					name = registrySecretName(cr)
				}
				if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "target-space", Name: name}, obj); !apierrors.IsNotFound(err) {
					t.Fatalf("teardown retained %T: %v", obj, err)
				}
			}
		})
	}
	b, cr := backendFixture(t)
	cr.Spec.ForProvider.SpaceName = nil
	cr.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(1, 0)))
	assertObservation(t, b, cr, false, false)
	if _, err := b.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
}

func TestKubernetesWorkloadNamePreflight(t *testing.T) {
	for _, name := range []string{strings.Repeat("a", 64), "invalid.name", "Invalid", "1-invalid-name", ""} {
		t.Run(name, func(t *testing.T) {
			b, cr := backendFixture(t)
			cr.Spec.ForProvider.Name = name
			counter := &writeCountingClient{Client: b.target}
			b.target = counter
			if _, err := b.Create(context.Background(), cr); err == nil {
				t.Fatal("invalid Service name accepted")
			}
			if counter.writes != 0 {
				t.Fatal("invalid Service name left partial workload")
			}
		})
	}
	for _, name := range []string{strings.Repeat("a", 63), "valid-name-1"} {
		b, cr := backendFixture(t)
		cr.Spec.ForProvider.Name = name
		if _, err := b.Create(context.Background(), cr); err != nil {
			t.Fatalf("ordinary valid name rejected: %v", err)
		}
	}
}

func TestKubernetesDeletingForeignObjectsPreservesOwnedCleanup(t *testing.T) {
	for _, foreignKind := range []string{"deployment", "service", "secret"} {
		t.Run(foreignKind, func(t *testing.T) {
			b, cr, _ := registryFixture(t)
			if _, err := b.Create(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			dep, svc := getWorkload(t, b, cr)
			secret := &corev1.Secret{}
			if err := b.target.Get(context.Background(), types.NamespacedName{Namespace: "target-space", Name: registrySecretName(cr)}, secret); err != nil {
				t.Fatal(err)
			}
			objects := map[string]k8s.Object{"deployment": dep, "service": svc, "secret": secret}
			foreign := objects[foreignKind]
			foreign.SetAnnotations(nil)
			foreign.SetUID("foreign-target-uid")
			if err := b.target.Update(context.Background(), foreign); err != nil {
				t.Fatal(err)
			}
			before := foreign.DeepCopyObject().(k8s.Object)
			cr.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(1, 0)))
			assertObservation(t, b, cr, true, false)
			if _, err := b.Delete(context.Background(), cr); err != nil {
				t.Fatal(err)
			}
			assertObservation(t, b, cr, false, false)
			for kind, obj := range objects {
				err := b.target.Get(context.Background(), k8s.ObjectKeyFromObject(obj), obj)
				if kind != foreignKind {
					if !apierrors.IsNotFound(err) {
						t.Fatalf("owned %s retained: %v", kind, err)
					}
					continue
				}
				if err != nil || obj.GetUID() != before.GetUID() || obj.GetResourceVersion() != before.GetResourceVersion() || !reflect.DeepEqual(obj.GetAnnotations(), before.GetAnnotations()) {
					t.Fatal("foreign object mutated or deleted")
				}
			}
		})
	}
}

func TestKubernetesUnpinnedCollisionTeardown(t *testing.T) {
	b, cr := backendFixture(t)
	foreign := b.buildService(cr)
	foreign.Annotations = nil
	foreign.UID = "foreign-service"
	if err := b.target.Create(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Observe(context.Background(), cr); err == nil {
		t.Fatal("ownership collision accepted")
	}
	if meta.GetExternalName(cr) != "" {
		t.Fatal("identity pinned before collision preflight completed")
	}
	cr.DeletionTimestamp = ptr.To(metav1.NewTime(time.Unix(1, 0)))
	assertObservation(t, b, cr, false, false)
	if _, err := b.Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if err := b.target.Get(context.Background(), k8s.ObjectKeyFromObject(foreign), &corev1.Service{}); err != nil {
		t.Fatal("foreign collision target removed")
	}
}
