//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/anocentino/sql-on-k8s-operator/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	// Override with IMG env var (e.g. IMG=sql-on-k8s-operator:latest).
	managerImage = "sql-on-k8s-operator:latest"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in a running cluster.
//
// Environment variables:
//   - IMG                     - override the operator image (default: sql-on-k8s-operator:latest)
//   - CERT_MANAGER_INSTALL_SKIP=true - skip cert-manager installation
//   - KIND_CLUSTER_SKIP=true  - skip Kind image loading (use this for Docker Desktop)
func TestE2E(t *testing.T) {
	if v := os.Getenv("IMG"); v != "" {
		managerImage = v
	}
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting sql-on-k8s-operator e2e test suite (image=%s)\n", managerImage)
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	// Build the operator image unless explicitly skipped.
	if os.Getenv("BUILD_IMAGE_SKIP") != "true" {
		By("building the manager image")
		cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")
	}

	// Only load into Kind when not using Docker Desktop.
	if os.Getenv("KIND_CLUSTER_SKIP") != "true" {
		By("loading the manager image on Kind")
		err := utils.LoadImageToKindClusterWithName(managerImage)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")
	}

	setupCertManager()
	deployOperator(managerImage)
	setupAG()
})

var _ = AfterSuite(func() {
	teardownAG()
	teardownOperator()
	teardownCertManager()
})

// deployOperator installs CRDs and deploys the controller-manager into the cluster.
// This is called once in BeforeSuite so all specs share the same operator instance.
func deployOperator(image string) {
	By("creating manager namespace")
	cmd := exec.Command("kubectl", "create", "ns", "sql-on-k8s-operator-system")
	_, _ = utils.Run(cmd) // idempotent: ignore error if namespace already exists

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", "sql-on-k8s-operator-system",
		"pod-security.kubernetes.io/enforce=restricted")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", image))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy controller-manager")

	By("waiting for controller-manager to be ready")
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pods",
			"-l", "control-plane=controller-manager",
			"-o", "jsonpath={.items[0].status.phase}",
			"-n", "sql-on-k8s-operator-system")
		out, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("Running"))
	}, 3*time.Minute, 5*time.Second).Should(Succeed(), "controller-manager pod never became Running")
}

// teardownOperator undeploys the controller-manager and uninstalls CRDs.
func teardownOperator() {
	By("undeploying the controller-manager")
	cmd := exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	By("removing manager namespace")
	cmd = exec.Command("kubectl", "delete", "ns", "sql-on-k8s-operator-system", "--ignore-not-found=true")
	_, _ = utils.Run(cmd)
}

// setupAG creates the AG secret and 3-replica AG CR, then waits for bootstrap to complete.
// Called once in BeforeSuite so all AG specs share the same running AG.
func setupAG() {
	By("ensuring the AG secret exists")
	_ = exec.Command("kubectl", "create", "secret", "generic", agSecretName,
		"--from-literal=SA_PASSWORD="+sqlPassword,
		"-n", agNamespace).Run()

	By("applying the 3-replica AG CR")
	f, err := os.CreateTemp("", "ag-cr-*.yaml")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	_, _ = f.WriteString(agCRYAML)
	f.Close()
	cmd := exec.Command("kubectl", "apply", "-f", f.Name())
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to apply AG CR")

	By("waiting for AG bootstrap to complete (up to 20 minutes)")
	waitForAGBootstrap("mssql-ag", 20*time.Minute)
}

// teardownAG deletes the AG CR and its PVCs.
func teardownAG() {
	By("deleting the AG CR")
	_ = exec.Command("kubectl", "delete", "sqlag", "mssql-ag",
		"-n", agNamespace, "--ignore-not-found=true").Run()
	By("deleting AG PVCs")
	_ = exec.Command("kubectl", "delete", "pvc", "-l", "app=mssql-ag",
		"-n", agNamespace).Run()
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
