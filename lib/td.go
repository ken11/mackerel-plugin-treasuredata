package mptd

import (
	"flag"
	"fmt"
	"os"

	mp "github.com/mackerelio/go-mackerel-plugin"
	td_client "github.com/treasure-data/td-client-go"
)

type TDPlugin struct {
	Prefix string
	client td_client.TDClient
}

type PluginWithPrefix interface {
	FetchMetrics() (map[string]float64, error)
	GraphDefinition() map[string]mp.Graphs
	MetricKeyPrefix() string
}

func get_databases(t TDPlugin) *td_client.ListDataBasesResult {
	databases, err := t.client.ListDatabases()
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	return databases
}

func get_tables(t TDPlugin, name string) *td_client.ListTablesResult {
	tables, err := t.client.ListTables(name)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	return tables
}

func get_jobs(t TDPlugin) *td_client.ListJobsResult {
	jobs, err := t.client.ListJobs()
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	return jobs
}

func (t TDPlugin) GraphDefinition() map[string]mp.Graphs {
	databases := get_databases(t)

	var db_metrics []mp.Metrics
	graphs := map[string]mp.Graphs{}

	for _, database := range *databases {
		db_metrics = append(db_metrics, mp.Metrics{Name: "db_" + database.Name, Label: database.Name})

		tables := get_tables(t, database.Name)
		var table_metrics []mp.Metrics
		for _, table := range *tables {
			table_metrics = append(table_metrics, mp.Metrics{Name: table.Name, Label: table.Name})
		}

		graphs[database.Name] = mp.Graphs{Label: "Count " + database.Name + " records", Unit: "integer", Metrics: table_metrics}
	}

	graphs["total_records"] = mp.Graphs{
		Label: "Count total records",
		Unit:  "integer",
		Metrics: [](mp.Metrics){
			mp.Metrics{Name: "total_records", Label: "Total Records"},
		},
	}

	graphs["jobs"] = mp.Graphs{
		Label: "Count running/queued jobs",
		Unit:  "integer",
		Metrics: [](mp.Metrics){
			mp.Metrics{Name: "queued", Label: "Queued"},
			mp.Metrics{Name: "running", Label: "Running"},
		},
	}

	graphs["records"] = mp.Graphs{
		Label:   "Count DB records",
		Unit:    "integer",
		Metrics: db_metrics,
	}

	return graphs
}

func (t TDPlugin) FetchMetrics() (map[string]float64, error) {
	jobs := get_jobs(t)
	databases := get_databases(t)
	stat := map[string]float64{"queued": 0, "running": 0, "total_records": 0}

	for _, database := range *databases {
		stat["db_"+database.Name] = float64(database.Count)
		stat["total_records"] += float64(database.Count)

		tables := get_tables(t, database.Name)
		for _, table := range *tables {
			stat[table.Name] = float64(table.Count)
		}
	}

	for _, job := range jobs.ListJobsResultElements {
		if job.Status == "queued" || job.Status == "running" {
			stat[job.Status]++
		}
	}

	return stat, nil
}

func (t TDPlugin) MetricKeyPrefix() string {
	if t.Prefix == "" {
		t.Prefix = "td"
	}
	return t.Prefix
}

func Do() {
	optPrefix := flag.String("metric-key-prefix", "td", "Metric key prefix")
	optApiKey := flag.String("td-apikey", "", "Treasure Data Api Key")
	flag.Parse()

	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: *optApiKey,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	t := TDPlugin{
		Prefix: *optPrefix,
		client: *client,
	}

	plugin := mp.NewMackerelPlugin(t)
	plugin.Run()
}
