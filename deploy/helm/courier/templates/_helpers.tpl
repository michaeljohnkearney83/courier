{{/* NATS URL for link state, or empty when there is none. */}}
{{- define "courier.natsUrl" -}}
{{- if .Values.nats.enabled -}}
nats://nats:4222
{{- else -}}
{{ .Values.nats.externalUrl }}
{{- end -}}
{{- end -}}

{{/* Every relay node as node=address, through the headless service. */}}
{{- define "courier.peers" -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.relayNode.replicas) -}}
{{- $peers = append $peers (printf "relay-node-%d=http://relay-node-%d.relay-node:%d" $i $i (int $.Values.relayNode.grpcPort)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{/* Run as the distroless nonroot user, with nothing writable but volumes. */}}
{{- define "courier.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}
