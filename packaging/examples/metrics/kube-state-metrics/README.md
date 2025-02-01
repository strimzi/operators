# Kube-state-metrics

This folder contains examples of how Strimzi integrates [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics)(KSM) for custom resources monitoring and demonstrates how they can be used.

[ConfigMap](./configmap.yaml):
* Contains the KSM configuration represented as `ConfigMap`
[PrometheusRules](./prometheus-rules.yaml)
* Contains the alerting based on metrics produced by KSM and collected by Prometheus
* Compatible with [Prometheus-Operator](https://github.com/prometheus-operator/prometheus-operator)
