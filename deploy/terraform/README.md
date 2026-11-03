# GKE with Terraform

The smallest Google Kubernetes Engine setup that runs the Courier chart: one
zonal cluster with a single spot `e2-medium` node and small standard disks.
There's no load balancer; reach the services with `kubectl port-forward`.

## What it costs

- GKE's management fee is waived for **one zonal cluster** per billing account.
- You pay for the node VM and the disks: very roughly **$0.01–0.04 an hour**
  with a spot node, about $10–25 a month if you forget to tear it down. Check
  the GCP pricing pages for current prices.
- A new Google Cloud account's **$300 free-trial credit** (90 days) covers
  this many times over. Google doesn't charge your card during the trial
  unless you upgrade to a paid account yourself.
- Set `billing_account` to add a budget alert (default $10/month).
- Tear everything down after each demo (see [Tear down](#tear-down)).

## One-time setup

1. Start the free trial at <https://cloud.google.com/free>, create a project,
   and note its project id.
2. Make the images public: on GitHub, open each package
   (`courier-relay-node`, `courier-link-control`) under your profile's
   **Packages**, then **Package settings → Change visibility → Public**.
   They're published by `.github/workflows/images.yml` on every push to `main`.
3. Sign in and install the kubectl auth plugin:

   ```bash
   gcloud auth login
   gcloud auth application-default login   # credentials Terraform uses
   gcloud config set project <project-id>
   gcloud components install gke-gcloud-auth-plugin
   ```

4. Fill in your settings:

   ```bash
   cd deploy/terraform
   cp terraform.tfvars.example terraform.tfvars   # gitignored
   ```

## Create the cluster

```bash
terraform init
terraform plan      # read what it will create
terraform apply     # about 5–10 minutes
$(terraform output -raw get_credentials)
```

Then install the chart from the repository root:

```bash
helm install courier deploy/helm/courier -f deploy/helm/courier/values-gke.yaml
kubectl rollout status statefulset/relay-node
```

The link-cut demo in the main README's "Kubernetes with kind and Helm" section
works the same way here.

## Tear down

Delete the relay nodes' volumes **before** destroying the cluster. Disks
created for PersistentVolumeClaims are not part of the Terraform state and can
outlive the cluster, and you keep paying for them.

```bash
helm uninstall courier
kubectl delete pvc --all          # deletes the nodes' disks
terraform destroy
```

Afterwards, `gcloud compute disks list` should show no `pvc-...` disks.
