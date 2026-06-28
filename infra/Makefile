# LA-Boom cluster preparation.
# Wraps the Ansible playbook (infra/ansible) and the preflight verifier.
# Edit infra/ansible/inventory.ini and group_vars/all.yml first.

ANSIBLE_DIR := ansible
PLAYBOOK    := site.yml
ANSIBLE     := cd $(ANSIBLE_DIR) && ansible-playbook $(PLAYBOOK)

.PHONY: help prep node nfs npu registry images k8s monitoring keda check verify ping

help:
	@echo "LA-Boom cluster prep targets:"
	@echo "  make ping      - SSH connectivity check to all hosts"
	@echo "  make check     - dry-run the full playbook (--check)"
	@echo "  make prep      - run the full cluster preparation"
	@echo "  make node      - per-node OS config only (DNS/registry/proxy)"
	@echo "  make nfs       - NFS server exports only"
	@echo "  make npu       - verify NPU driver stack on workers"
	@echo "  make registry  - deploy the private registry"
	@echo "  make images    - build + push + mirror images"
	@echo "  make k8s       - cluster bootstrap (taint/labels/PV/LWS)"
	@echo "  make monitoring - deploy Prometheus + Grafana stack"
	@echo "  make keda      - install KEDA + LWS scale RBAC (autoscaling only)"
	@echo "  make verify    - run preflight checks (read-only)"

ping:
	cd $(ANSIBLE_DIR) && ansible all -m ping

check:
	$(ANSIBLE) --check --diff

prep:
	$(ANSIBLE)

node:
	$(ANSIBLE) --tags node

nfs:
	$(ANSIBLE) --tags nfs

npu:
	$(ANSIBLE) --tags npu

registry:
	$(ANSIBLE) --tags registry

images:
	$(ANSIBLE) --tags images

k8s:
	$(ANSIBLE) --tags k8s

monitoring:
	$(ANSIBLE) --tags monitoring

keda:
	$(ANSIBLE) --tags keda

verify:
	./preflight.sh
