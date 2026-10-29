/*
Copyright 2023 SAP SE.
*/

package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/SAP/crossplane-provider-cloudfoundry/apis/resources/v1alpha1"
	pcv1beta1 "github.com/SAP/crossplane-provider-cloudfoundry/apis/v1beta1"
	"github.com/docker/cli/cli/config/configfile"
)

const (
	// lifecycleDocker is the only App lifecycle supported by the Kubernetes backend.
	lifecycleDocker = "docker"

	// webProcessType is the CF process type whose instance count maps to the
	// Deployment replica count.
	webProcessType = "web"
	// defaultContainerPort is used when Environment.PORT is absent.
	defaultContainerPort = 8080
	ownedEnvAnnotation   = "cloudfoundry.crossplane.io/owned-env"
	ownerUIDAnnotation   = "cloudfoundry.crossplane.io/app-uid"
	appUIDLabel          = "cloudfoundry.crossplane.io/app-uid"
	// managedByLabel/appLabel correlate the Deployment and Service back to the
	// App managed resource.
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "crossplane-provider-cloudfoundry"
	appNameLabel   = "cloudfoundry.crossplane.io/app"

	errBuildTargetClient = "cannot build Kubernetes client from ProviderConfig kubeconfig"
	errNotDockerK8s      = "Kubernetes backend only supports docker-lifecycle Apps"
	errNoImage           = "docker image is required (spec.forProvider.docker.image)"
	errNoNamespace       = "a namespace is required: set spec.forProvider.spaceName (interpreted as the target namespace)"
	errApplyDeployment   = "cannot apply Deployment"
	errApplyService      = "cannot apply Service"
	errGetDeployment     = "cannot get Deployment"
	errDeleteWorkload    = "cannot delete workload"
)

// newKubeBackendClient builds a controller-runtime client for the target
// cluster from the kubeconfig stored in the App's ProviderConfig credentials.
func newKubeBackendClient(ctx context.Context, kube k8s.Client, cr *v1alpha1.App) (k8s.Client, http.RoundTripper, error) {
	ref := cr.GetProviderConfigReference()
	if ref == nil || ref.Name == "" {
		return nil, nil, errors.New("providerConfigRef is required")
	}
	pc := &pcv1beta1.ProviderConfig{}
	if err := kube.Get(ctx, types.NamespacedName{Name: ref.Name}, pc); err != nil {
		return nil, nil, errors.Wrap(err, "cannot get ProviderConfig")
	}
	if pc.Spec.Credentials.Source != xpv2.CredentialsSourceSecret {
		return nil, nil, errors.New("Kubernetes backend kubeconfig credentials must use Secret source; provider filesystem and executable credential sources are forbidden")
	}
	kubeconfig, err := resource.CommonCredentialExtractor(ctx, pc.Spec.Credentials.Source, kube, pc.Spec.Credentials.CommonCredentialSelectors)
	if err != nil {
		return nil, nil, errors.New("cannot read ProviderConfig kubeconfig Secret")
	}
	config, err := safeKubeconfig(kubeconfig)
	if err != nil {
		return nil, nil, err
	}
	restCfg, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, nil, errors.New(errBuildTargetClient + ": invalid inline client configuration")
	}
	var rt http.RoundTripper
	restCfg.Wrap(func(inner http.RoundTripper) http.RoundTripper { rt = inner; return inner })
	cl, err := k8s.New(restCfg, k8s.Options{Scheme: scheme.Scheme})
	if err != nil {
		if closer, ok := rt.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
		return nil, nil, errors.New(errBuildTargetClient + ": cannot initialize target client")
	}
	return cl, rt, nil
}

// k8sExternal reconciles an App onto a target Kubernetes cluster as a
// Deployment (plus a ClusterIP Service). It implements managed.ExternalClient.
type k8sExternal struct {
	// target is the client for the cluster the App is materialized on.
	target k8s.Client
	// kube is the control-plane client (where the App CR and any referenced
	// secrets live).
	kube      k8s.Client
	rt        http.RoundTripper
	namespace string
}

// workloadName/namespace derive the target object identity from the App spec.
// The external-name is namespace/name so that Observe (which runs before
// Create) can locate an existing workload deterministically.
func workloadNamespace(cr *v1alpha1.App) string {
	if cr.Spec.ForProvider.SpaceName != nil {
		return *cr.Spec.ForProvider.SpaceName
	}
	return ""
}

func workloadName(cr *v1alpha1.App) string {
	return cr.Spec.ForProvider.Name
}

func externalName(ns, name string) string {
	return ns + "/" + name
}

func parseExternalName(xn string) (namespace, name string, ok bool) {
	parts := strings.Split(xn, "/")
	if len(parts) != 2 || len(validation.IsDNS1123Label(parts[0])) != 0 || len(validation.IsDNS1123Subdomain(parts[1])) != 0 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// validateDockerApp rejects Apps the Kubernetes backend cannot materialize.
func validateDockerApp(cr *v1alpha1.App) error {
	if cr.Spec.ForProvider.Lifecycle != "" && cr.Spec.ForProvider.Lifecycle != lifecycleDocker {
		return errors.New(errNotDockerK8s)
	}
	if cr.Spec.ForProvider.Docker == nil || cr.Spec.ForProvider.Docker.Image == "" {
		return errors.New(errNoImage)
	}
	return nil
}

func (c *k8sExternal) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.App)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errWrongKind)
	}
	if meta.WasDeleted(cr) {
		if cr.UID == "" {
			return managed.ExternalObservation{}, errors.New("App UID is required for Kubernetes ownership")
		}
		key, found, err := c.deletionIdentity(ctx, cr)
		if err != nil {
			return managed.ExternalObservation{}, err
		}
		if !found {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		dep, svc, secret, err := c.inspect(ctx, cr, key, true)
		if err != nil {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{ResourceExists: dep != nil || svc != nil || secret != nil}, nil
	}
	late := meta.GetExternalName(cr) == ""
	key, secret, err := c.prepare(ctx, cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	dep, svc, copied, err := c.inspect(ctx, cr, key, false)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	pinIdentity(cr, key)
	if dep == nil {
		return managed.ExternalObservation{ResourceLateInitialized: late}, nil
	}
	c.updateObservedStatus(cr, dep)
	desired := c.buildDeployment(cr)
	current := svc != nil && deploymentUpToDate(dep, desired) && serviceUpToDate(svc, c.buildService(cr)) && registryUpToDate(copied, secret)
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: current, ResourceLateInitialized: late}, nil
}

func (c *k8sExternal) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.App)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errWrongKind)
	}
	key, secret, err := c.prepare(ctx, cr)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	if _, _, _, err := c.inspect(ctx, cr, key, false); err != nil {
		return managed.ExternalCreation{}, err
	}
	pinIdentity(cr, key)
	cr.SetConditions(xpv2.Creating())
	if secret != nil {
		if err := c.createOwned(ctx, cr, secret); err != nil {
			return managed.ExternalCreation{}, err
		}
	}
	for _, obj := range []k8s.Object{c.buildDeployment(cr), c.buildService(cr)} {
		if err := c.createOwned(ctx, cr, obj); err != nil {
			return managed.ExternalCreation{}, err
		}
	}
	return managed.ExternalCreation{}, nil
}

func (c *k8sExternal) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.App)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errWrongKind)
	}
	key, secret, err := c.prepare(ctx, cr)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	existing, svc, copied, err := c.inspect(ctx, cr, key, false)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	if existing == nil {
		return managed.ExternalUpdate{}, errors.New("cannot update missing Deployment")
	}
	pinIdentity(cr, key)
	desired := c.buildDeployment(cr)
	if existing.Spec.Selector == nil || !mapsEqual(existing.Spec.Selector.MatchLabels, desired.Spec.Selector.MatchLabels) || len(existing.Spec.Selector.MatchExpressions) != 0 {
		return managed.ExternalUpdate{}, errors.New("owned Deployment has incompatible immutable selector")
	}
	liveApp, wantApp := appContainer(existing), appContainer(desired)
	if liveApp == nil {
		return managed.ExternalUpdate{}, errors.New("app container is missing")
	}
	previous, err := ownedEnvironment(existing)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	owned := make(map[string]struct{}, len(previous)+len(wantApp.Env))
	for _, name := range previous {
		owned[name] = struct{}{}
	}
	for _, variable := range wantApp.Env {
		owned[variable.Name] = struct{}{}
	}
	env := make([]corev1.EnvVar, 0, len(liveApp.Env)+len(wantApp.Env))
	for _, variable := range liveApp.Env {
		if _, managed := owned[variable.Name]; !managed {
			env = append(env, variable)
		}
	}
	liveApp.Env = append(env, wantApp.Env...)
	liveApp.Image, liveApp.Command, liveApp.Ports = wantApp.Image, wantApp.Command, wantApp.Ports
	existing.Spec.Replicas = desired.Spec.Replicas
	if existing.Spec.Template.Labels == nil {
		existing.Spec.Template.Labels = map[string]string{}
	}
	for name, value := range desired.Spec.Template.Labels {
		existing.Spec.Template.Labels[name] = value
	}
	if existing.Spec.Template.Annotations == nil {
		existing.Spec.Template.Annotations = map[string]string{}
	}
	existing.Spec.Template.Annotations[ownedEnvAnnotation] = desired.Spec.Template.Annotations[ownedEnvAnnotation]
	existing.Spec.Template.Spec.ImagePullSecrets = reconcilePullRefs(existing.Spec.Template.Spec.ImagePullSecrets, desired.Spec.Template.Spec.ImagePullSecrets, registrySecretName(cr))
	if secret != nil {
		if copied == nil {
			err = c.createOwned(ctx, cr, secret)
		} else {
			copied.Type, copied.Data = secret.Type, secret.Data
			err = c.target.Update(ctx, copied)
		}
		if err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, "cannot apply registry Secret")
		}
	}
	// Keep the live pull reference until Secret deletion succeeds, so failures
	// remain discoverable and retryable after credentials are removed from App.
	if secret == nil && copied != nil {
		if err := c.deleteOwned(ctx, copied); err != nil {
			return managed.ExternalUpdate{}, err
		}
	}
	if err := c.target.Update(ctx, existing); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errApplyDeployment)
	}
	if svc == nil {
		err = c.createOwned(ctx, cr, c.buildService(cr))
	} else {
		want := c.buildService(cr)
		svc.Spec.Type, svc.Spec.Ports, svc.Spec.Selector = want.Spec.Type, want.Spec.Ports, want.Spec.Selector
		err = c.target.Update(ctx, svc)
	}
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errApplyService)
	}
	return managed.ExternalUpdate{}, nil
}

func (c *k8sExternal) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	cr, ok := mg.(*v1alpha1.App)
	if !ok {
		return managed.ExternalDelete{}, errors.New(errWrongKind)
	}
	if cr.UID == "" {
		return managed.ExternalDelete{}, errors.New("App UID is required for Kubernetes ownership")
	}
	key, found, err := c.deletionIdentity(ctx, cr)
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	if !found {
		return managed.ExternalDelete{}, nil
	}
	dep, svc, secret, err := c.inspect(ctx, cr, key, true)
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	cr.SetConditions(xpv2.Deleting())
	objects := []k8s.Object{}
	if svc != nil {
		objects = append(objects, svc)
	}
	if secret != nil {
		objects = append(objects, secret)
	}
	if dep != nil {
		objects = append(objects, dep)
	}
	for _, obj := range objects {
		if err := c.deleteOwned(ctx, obj); err != nil {
			return managed.ExternalDelete{}, err
		}
	}
	return managed.ExternalDelete{}, nil
}

func (c *k8sExternal) Disconnect(_ context.Context) error {
	if closer, ok := c.rt.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	return nil
}

// updateObservedStatus reports only fully converged, available rollouts as STARTED.
func (c *k8sExternal) updateObservedStatus(cr *v1alpha1.App, dep *appsv1.Deployment) {
	cr.Status.AtProvider.Name, cr.Status.AtProvider.GUID = dep.Name, string(dep.UID)
	cr.Status.AtProvider.AppManifest, cr.Status.AtProvider.Routes = "", nil
	requested := desiredReplicas(cr)
	if requested > 0 && ptr.Deref(dep.Spec.Replicas, 1) == requested && dep.Status.ObservedGeneration >= dep.Generation && dep.Status.UpdatedReplicas == requested && dep.Status.AvailableReplicas == requested && dep.Status.ReadyReplicas == requested && dep.Status.Replicas == requested {
		cr.Status.AtProvider.State = "STARTED"
		cr.SetConditions(xpv2.Available())
	} else {
		cr.Status.AtProvider.State = "STOPPED"
		cr.SetConditions(xpv2.Unavailable())
	}
}

// desiredReplicas maps the web process instance count to replicas, defaulting
// to 1.
func desiredReplicas(cr *v1alpha1.App) int32 {
	for _, p := range cr.Spec.ForProvider.Processes {
		if p.Type != nil && *p.Type == webProcessType && p.Instances != nil {
			return int32(*p.Instances) //nolint:gosec // instance counts are small
		}
	}
	return 1
}

// webCommand returns the container command from the web process, if any.
func webCommand(cr *v1alpha1.App) []string {
	for _, p := range cr.Spec.ForProvider.Processes {
		if p.Type != nil && *p.Type == webProcessType && p.Command != nil && *p.Command != "" {
			return []string{"/bin/sh", "-c", *p.Command}
		}
	}
	return nil
}

func envVars(cr *v1alpha1.App) []corev1.EnvVar {
	keys := make([]string, 0, len(cr.Spec.ForProvider.Environment))
	for k := range cr.Spec.ForProvider.Environment {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic ordering for up-to-date comparison
	out := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		out = append(out, corev1.EnvVar{Name: k, Value: cr.Spec.ForProvider.Environment[k]})
	}
	return out
}

func labels(cr *v1alpha1.App) map[string]string {
	return map[string]string{
		managedByLabel: managedByValue,
		appUIDLabel:    ownerLabel(cr),
	}
}

func (c *k8sExternal) buildDeployment(cr *v1alpha1.App) *appsv1.Deployment {
	port, _ := desiredPort(cr) // Entry points validate before constructing or writing objects.
	env := envVars(cr)
	keys := make([]string, 0, len(env))
	for _, variable := range env {
		keys = append(keys, variable.Name)
	}
	raw, _ := json.Marshal(keys) // A slice of strings cannot fail JSON encoding.
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.namespaceFor(cr), Name: workloadName(cr), Labels: labels(cr), Annotations: ownerAnnotations(cr)},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(desiredReplicas(cr)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appUIDLabel: ownerLabel(cr)}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(cr), Annotations: map[string]string{ownedEnvAnnotation: string(raw)}},
				Spec: corev1.PodSpec{
					ImagePullSecrets: desiredPullRefs(cr),
					Containers: []corev1.Container{{
						Name: "app", Image: cr.Spec.ForProvider.Docker.Image, Command: webCommand(cr), Env: env,
						Ports: []corev1.ContainerPort{{ContainerPort: port, Protocol: corev1.ProtocolTCP}},
					}},
				},
			},
		},
	}
}

func (c *k8sExternal) buildService(cr *v1alpha1.App) *corev1.Service {
	port, _ := desiredPort(cr)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.namespaceFor(cr), Name: workloadName(cr), Labels: labels(cr), Annotations: ownerAnnotations(cr)},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{appUIDLabel: ownerLabel(cr)},
			Ports:    []corev1.ServicePort{{Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP}},
		},
	}
}

func appContainer(dep *appsv1.Deployment) *corev1.Container {
	for i := range dep.Spec.Template.Spec.Containers {
		if dep.Spec.Template.Spec.Containers[i].Name == "app" {
			return &dep.Spec.Template.Spec.Containers[i]
		}
	}
	return nil
}

func ownedEnvironment(dep *appsv1.Deployment) ([]string, error) {
	raw := dep.Spec.Template.Annotations[ownedEnvAnnotation]
	if raw == "" {
		return nil, nil
	} // Legacy objects adopt only currently desired keys.
	var names []string
	if err := json.Unmarshal([]byte(raw), &names); err != nil {
		return nil, errors.Wrap(err, "cannot read owned environment annotation")
	}
	return names, nil
}

func deploymentUpToDate(live, desired *appsv1.Deployment) bool {
	got, want := appContainer(live), appContainer(desired)
	if got == nil || want == nil || got.Image != want.Image || !equalStrings(got.Command, want.Command) {
		return false
	}
	if len(got.Ports) != 1 || got.Ports[0].ContainerPort != want.Ports[0].ContainerPort || got.Ports[0].Protocol != corev1.ProtocolTCP {
		return false
	}
	if live.Spec.Selector == nil || !mapsEqual(live.Spec.Selector.MatchLabels, desired.Spec.Selector.MatchLabels) || len(live.Spec.Selector.MatchExpressions) != 0 {
		return false
	}
	for name, value := range desired.Spec.Template.Labels {
		if live.Spec.Template.Labels[name] != value {
			return false
		}
	}
	secretName := ""
	if len(desired.Spec.Template.Spec.ImagePullSecrets) != 0 {
		secretName = desired.Spec.Template.Spec.ImagePullSecrets[0].Name
	}
	for _, ref := range live.Spec.Template.Spec.ImagePullSecrets {
		if strings.HasPrefix(ref.Name, "cf-app-") && ref.Name == registryNameFromUID(desired.Annotations[ownerUIDAnnotation]) {
			if secretName == "" {
				return false
			}
			secretName = ""
		}
	}
	if secretName != "" {
		return false
	}
	previous, err := ownedEnvironment(live)
	if err != nil {
		return false
	}
	desiredEnv := make(map[string]corev1.EnvVar, len(want.Env))
	for _, variable := range want.Env {
		desiredEnv[variable.Name] = variable
	}
	owned := make(map[string]struct{}, len(previous)+len(want.Env))
	for _, name := range previous {
		owned[name] = struct{}{}
	}
	for name := range desiredEnv {
		owned[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(want.Env))
	for _, variable := range got.Env {
		if _, managed := owned[variable.Name]; !managed {
			continue
		}
		wantVariable, exists := desiredEnv[variable.Name]
		if !exists || variable.Value != wantVariable.Value || variable.ValueFrom != nil {
			return false
		}
		if _, duplicate := seen[variable.Name]; duplicate {
			return false
		}
		seen[variable.Name] = struct{}{}
	}
	return len(seen) == len(desiredEnv) && live.Spec.Template.Annotations[ownedEnvAnnotation] == desired.Spec.Template.Annotations[ownedEnvAnnotation] && ptr.Deref(live.Spec.Replicas, 1) == ptr.Deref(desired.Spec.Replicas, 1)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func desiredPort(cr *v1alpha1.App) (int32, error) {
	value, ok := cr.Spec.ForProvider.Environment["PORT"]
	if !ok {
		return defaultContainerPort, nil
	}
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("invalid TCP port in Environment.PORT %q: expected integer 1..65535", value)
	}
	return int32(port), nil
}

func serviceUpToDate(live, desired *corev1.Service) bool {
	return live.Spec.Type == corev1.ServiceTypeClusterIP && len(live.Spec.Ports) == 1 && live.Spec.Ports[0].Port == desired.Spec.Ports[0].Port && live.Spec.Ports[0].TargetPort == desired.Spec.Ports[0].TargetPort && live.Spec.Ports[0].Protocol == corev1.ProtocolTCP && mapsEqual(live.Spec.Selector, desired.Spec.Selector)
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if got, ok := b[key]; !ok || got != value {
			return false
		}
	}
	return true
}

var _ managed.ExternalClient = &k8sExternal{}

func safeKubeconfig(data []byte) (*clientcmdapi.Config, error) {
	config, err := clientcmd.Load(data)
	if err != nil {
		return nil, errors.New("invalid ProviderConfig kubeconfig syntax")
	}
	context, ok := config.Contexts[config.CurrentContext]
	if !ok || context == nil {
		return nil, errors.New("kubeconfig current-context must reference an existing context")
	}
	if cluster, ok := config.Clusters[context.Cluster]; !ok || cluster == nil || cluster.Server == "" {
		return nil, errors.New("kubeconfig context must reference an existing cluster with server")
	}
	if context.AuthInfo != "" {
		if auth, ok := config.AuthInfos[context.AuthInfo]; !ok || auth == nil {
			return nil, errors.New("kubeconfig context references missing user")
		}
	}
	for _, auth := range config.AuthInfos {
		if auth == nil {
			return nil, errors.New("kubeconfig contains nil user")
		}
		if auth.Exec != nil || auth.AuthProvider != nil || auth.TokenFile != "" || auth.ClientCertificate != "" || auth.ClientKey != "" {
			return nil, errors.New("unsafe kubeconfig user: exec, auth-provider, tokenFile and certificate/key filesystem paths are forbidden; use inline credentials")
		}
	}
	for _, cluster := range config.Clusters {
		if cluster == nil || cluster.CertificateAuthority != "" {
			return nil, errors.New("unsafe kubeconfig cluster: certificate-authority filesystem paths are forbidden; use inline certificate-authority-data")
		}
	}
	return config, nil
}

func resolveKubernetesNamespace(ctx context.Context, kube k8s.Client, cr *v1alpha1.App) (string, error) {
	ref := cr.Spec.ForProvider.SpaceReference
	name := ""
	if ref.SpaceName != nil && *ref.SpaceName != "" {
		name = *ref.SpaceName
	} else {
		if kube == nil {
			return "", errors.New("cannot map Space: controlplane client is required")
		}
		if ref.SpaceRef != nil {
			space := &v1alpha1.Space{}
			if err := kube.Get(ctx, types.NamespacedName{Name: ref.SpaceRef.Name}, space); err != nil {
				return "", errors.Wrap(err, "cannot map referenced Space")
			}
			name = space.Spec.ForProvider.Name
		} else {
			spaces := &v1alpha1.SpaceList{}
			options := []k8s.ListOption{}
			if ref.SpaceSelector != nil {
				options = append(options, k8s.MatchingLabels(ref.SpaceSelector.MatchLabels))
			} else if ref.Space == nil || *ref.Space == "" {
				return "", errors.New("cannot map Space: set spaceName, spaceRef, spaceSelector or a locally managed Space GUID")
			}
			if err := kube.List(ctx, spaces, options...); err != nil {
				return "", errors.Wrap(err, "cannot list managed Spaces")
			}
			matches := 0
			for i := range spaces.Items {
				space := &spaces.Items[i]
				if ref.SpaceSelector == nil {
					if space.GetID() != *ref.Space {
						continue
					}
				} else if ptr.Deref(ref.SpaceSelector.MatchControllerRef, false) {
					appOwner, spaceOwner := metav1.GetControllerOf(cr), metav1.GetControllerOf(space)
					if appOwner == nil || spaceOwner == nil || appOwner.UID != spaceOwner.UID {
						continue
					}
				}
				name = space.Spec.ForProvider.Name
				matches++
			}
			if matches != 1 {
				return "", fmt.Errorf("cannot map Space: expected exactly one local managed Space, found %d", matches)
			}
		}
	}
	if len(validation.IsDNS1123Label(name)) != 0 {
		return "", errors.New("mapped Space name is not a valid Kubernetes namespace")
	}
	return name, nil
}

func (c *k8sExternal) namespaceFor(cr *v1alpha1.App) string {
	if c.namespace != "" {
		return c.namespace
	}
	return workloadNamespace(cr)
}

func (c *k8sExternal) identity(ctx context.Context, cr *v1alpha1.App, deleting bool) (types.NamespacedName, error) {
	pinned := meta.GetExternalName(cr)
	if pinned != "" {
		ns, name, ok := parseExternalName(pinned)
		if !ok {
			return types.NamespacedName{}, errors.New("Kubernetes backend mismatch or invalid external-name: expected namespace/name; legacy CF GUIDs cannot be adopted or deleted by this backend")
		}
		if deleting {
			c.namespace = ns
			return types.NamespacedName{Namespace: ns, Name: name}, nil
		}
	}
	ns, err := resolveKubernetesNamespace(ctx, c.kube, cr)
	if err != nil {
		return types.NamespacedName{}, err
	}
	name := workloadName(cr)
	if len(validation.IsDNS1035Label(name)) != 0 {
		return types.NamespacedName{}, errors.New("App workload name must follow standard Service DNS-1035 naming: at most 63 lowercase letters, digits or hyphens, beginning with a letter and ending with a letter or digit")
	}
	if pinned != "" && pinned != externalName(ns, name) {
		return types.NamespacedName{}, fmt.Errorf("Kubernetes App identity is pinned to %s; namespace/name cannot change: restore the original spec or create a new App; deletion cleans the pinned workload", pinned)
	}
	c.namespace = ns
	return types.NamespacedName{Namespace: ns, Name: name}, nil
}

func (c *k8sExternal) prepare(ctx context.Context, cr *v1alpha1.App) (types.NamespacedName, *corev1.Secret, error) {
	if cr.UID == "" {
		return types.NamespacedName{}, nil, errors.New("App UID is required for Kubernetes ownership")
	}
	if err := validateDockerApp(cr); err != nil {
		return types.NamespacedName{}, nil, err
	}
	if _, err := desiredPort(cr); err != nil {
		return types.NamespacedName{}, nil, err
	}
	key, err := c.identity(ctx, cr, false)
	if err != nil {
		return key, nil, err
	}
	secret, err := c.desiredRegistrySecret(ctx, cr)
	return key, secret, err
}

func ownerLabel(cr *v1alpha1.App) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(cr.UID)))[:32]
}
func ownerAnnotations(cr *v1alpha1.App) map[string]string {
	return map[string]string{ownerUIDAnnotation: string(cr.UID)}
}
func registryNameFromUID(uid string) string {
	return fmt.Sprintf("cf-app-%x", sha256.Sum256([]byte(uid)))[:39]
}
func registrySecretName(cr *v1alpha1.App) string { return registryNameFromUID(string(cr.UID)) }

func checkOwner(cr *v1alpha1.App, obj k8s.Object) error {
	if cr.UID == "" || obj.GetAnnotations()[ownerUIDAnnotation] != string(cr.UID) {
		return fmt.Errorf("Kubernetes ownership collision for %T %s/%s: object is not owned by this App UID", obj, obj.GetNamespace(), obj.GetName())
	}
	return nil
}

func (c *k8sExternal) inspect(ctx context.Context, cr *v1alpha1.App, key types.NamespacedName, deleting bool) (*appsv1.Deployment, *corev1.Service, *corev1.Secret, error) {
	dep, svc := &appsv1.Deployment{}, &corev1.Service{}
	for _, obj := range []k8s.Object{dep, svc} {
		err := c.target.Get(ctx, key, obj)
		if apierrors.IsNotFound(err) {
			if obj == dep {
				dep = nil
			} else {
				svc = nil
			}
			continue
		}
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "cannot inspect Kubernetes workload ownership")
		}
		if err := checkOwner(cr, obj); err != nil {
			if deleting {
				if obj == dep {
					dep = nil
				} else {
					svc = nil
				}
				continue
			}
			return nil, nil, nil, err
		}
	}
	wantRegistry := cr.Spec.ForProvider.Docker != nil && cr.Spec.ForProvider.Docker.Credentials != nil
	if dep != nil {
		for _, ref := range dep.Spec.Template.Spec.ImagePullSecrets {
			if ref.Name == registrySecretName(cr) {
				wantRegistry = true
			}
		}
	}
	if !wantRegistry {
		return dep, svc, nil, nil
	}
	secret := &corev1.Secret{}
	err := c.target.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: registrySecretName(cr)}, secret)
	if apierrors.IsNotFound(err) {
		return dep, svc, nil, nil
	}
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "cannot inspect owned registry Secret")
	}
	if err := checkOwner(cr, secret); err != nil {
		if deleting {
			return dep, svc, nil, nil
		}
		return nil, nil, nil, err
	}
	return dep, svc, secret, nil
}

func (c *k8sExternal) createOwned(ctx context.Context, cr *v1alpha1.App, obj k8s.Object) error {
	if err := c.target.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrap(err, "cannot create Kubernetes workload object")
		}
		live := obj.DeepCopyObject().(k8s.Object)
		if err := c.target.Get(ctx, k8s.ObjectKeyFromObject(obj), live); err != nil {
			return err
		}
		return checkOwner(cr, live)
	}
	return nil
}

func (c *k8sExternal) deleteOwned(ctx context.Context, obj k8s.Object) error {
	uid, rv := obj.GetUID(), obj.GetResourceVersion()
	err := c.target.Delete(ctx, obj, &k8s.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "cannot delete owned Kubernetes object with UID precondition")
	}
	return nil
}

func (c *k8sExternal) desiredRegistrySecret(ctx context.Context, cr *v1alpha1.App) (*corev1.Secret, error) {
	ref := cr.Spec.ForProvider.Docker.Credentials
	if ref == nil {
		return nil, nil
	}
	if c.kube == nil {
		return nil, errors.New("cannot read registry credentials: controlplane client is required")
	}
	source := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, source); err != nil {
		return nil, errors.New("cannot read Docker credentials controlplane Secret")
	}
	data := source.Data[corev1.DockerConfigJsonKey]
	var config struct {
		Auths map[string]json.RawMessage `json:"auths"`
	}
	if len(data) == 0 || json.Unmarshal(data, &config) != nil || len(config.Auths) == 0 {
		return nil, errors.New("Docker credentials Secret must contain valid .dockerconfigjson with registry auths")
	}
	parsed := configfile.New("")
	if err := parsed.LoadFromReader(bytes.NewReader(data)); err != nil {
		return nil, errors.New("Docker credentials Secret contains malformed registry authentication")
	}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: c.namespaceFor(cr), Name: registrySecretName(cr), Annotations: ownerAnnotations(cr)}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: data}}, nil
}

func registryUpToDate(live, desired *corev1.Secret) bool {
	if desired == nil {
		return live == nil
	}
	return live != nil && live.Type == desired.Type && bytes.Equal(live.Data[corev1.DockerConfigJsonKey], desired.Data[corev1.DockerConfigJsonKey])
}

func desiredPullRefs(cr *v1alpha1.App) []corev1.LocalObjectReference {
	if cr.Spec.ForProvider.Docker.Credentials == nil {
		return nil
	}
	return []corev1.LocalObjectReference{{Name: registrySecretName(cr)}}
}

func reconcilePullRefs(live, desired []corev1.LocalObjectReference, ownedName string) []corev1.LocalObjectReference {
	out := make([]corev1.LocalObjectReference, 0, len(live)+len(desired))
	for _, ref := range live {
		if ref.Name != ownedName {
			out = append(out, ref)
		}
	}
	return append(out, desired...)
}

func (c *k8sExternal) deletionIdentity(ctx context.Context, cr *v1alpha1.App) (types.NamespacedName, bool, error) {
	if meta.GetExternalName(cr) != "" {
		key, err := c.identity(ctx, cr, true)
		return key, err == nil, err
	}
	// Identity is pinned before this backend's first target write. Unpinned
	// resources include failed ownership collisions and cannot own a workload.
	return types.NamespacedName{}, false, nil
}

func pinIdentity(cr *v1alpha1.App, key types.NamespacedName) {
	if meta.GetExternalName(cr) == "" {
		meta.SetExternalName(cr, externalName(key.Namespace, key.Name))
	}
}
