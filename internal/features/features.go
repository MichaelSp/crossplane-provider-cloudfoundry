/*
 Copyright 2022 Upbound Inc
*/

package features

import "github.com/crossplane/crossplane-runtime/v2/pkg/feature"

// Feature flags.
const (
	// EnableBetaManagementPolicies enables beta support for
	// Management Policies. See the below design for more details.
	// https://github.com/crossplane/crossplane/pull/3531
	EnableBetaManagementPolicies feature.Flag = "EnableBetaManagementPolicies"

	// EnableKubernetesAppBackend routes App reconciliation to a Kubernetes
	// backend that materializes the App as a Deployment on a target cluster
	// instead of pushing it to Cloud Foundry. Only docker-lifecycle Apps are
	// supported; buildpack Apps are rejected. The target cluster is selected
	// via a kubeconfig secret referenced by the App's ProviderConfig.
	EnableKubernetesAppBackend feature.Flag = "EnableKubernetesAppBackend"
)
