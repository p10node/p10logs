package enrich

import (
	"encoding/json"
	"testing"
)

func TestOwner(t *testing.T) {
	cases := map[string]string{
		`{"metadata":{"name":"api-7bd76897d8-4rkn2","ownerReferences":[{"kind":"ReplicaSet","name":"api-7bd76897d8"}]}}`: "Deployment/api",
		`{"metadata":{"name":"db-0","ownerReferences":[{"kind":"StatefulSet","name":"db"}]}}`:                            "StatefulSet/db",
		`{"metadata":{"name":"p10logs-agent-k5zs2","ownerReferences":[{"kind":"DaemonSet","name":"p10logs-agent"}]}}`:    "DaemonSet/p10logs-agent",
		`{"metadata":{"name":"report-29851812-mpdkl","ownerReferences":[{"kind":"Job","name":"report-29851812"}]}}`:      "CronJob/report",
		`{"metadata":{"name":"migrate-x7k2p","ownerReferences":[{"kind":"Job","name":"migrate"}]}}`:                      "Job/migrate",
		`{"metadata":{"name":"etcd-node1","ownerReferences":[{"kind":"Node","name":"node1"}]}}`:                          "Pod/etcd",
		`{"metadata":{"name":"debug"}}`: "Pod/debug",
	}
	for in, want := range cases {
		var p podObj
		if err := json.Unmarshal([]byte(in), &p); err != nil {
			t.Fatal(err)
		}
		if got := Owner(&p); got != want {
			t.Errorf("%s: got %s want %s", p.Metadata.Name, got, want)
		}
	}
}
