package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func newHandler(c Config) http.Handler {
	a := application{c}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"allow_delete": c.AllowDelete, "page_size": c.PageSize})
	})
	mux.HandleFunc("GET /api/instances", func(w http.ResponseWriter, r *http.Request) {
		items := []any{}
		for id, i := range c.Instances {
			items = append(items, map[string]any{"id": id, "name": i.Name, "mode": i.Mode})
		}
		writeJSON(w, items)
	})

	for _, route := range []struct{ pattern, operation string }{
		{"GET /api/instances/{id}/databases", "databases"}, {"GET /api/instances/{id}/keys", "keys"}, {"DELETE /api/instances/{id}/keys", "delete"}, {"GET /api/instances/{id}/value", "value"}, {"GET /api/instances/{id}/download", "export"},
	} {
		mux.HandleFunc(route.pattern, a.httpOperation(route.operation))
	}
	mux.Handle("/mcp", a.mcpHandler())
	return mux
}
func (a application) httpOperation(operation string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			http.Error(w, "Unknown Redis instance", 404)
			return
		}
		q := r.URL.Query()
		in := redisOperationInput{Prefix: q.Get("prefix"), View: q.Get("view")}
		if raw, ok := q["db"]; ok {
			db, e := strconv.Atoi(raw[0])
			if e != nil {
				http.Error(w, "Invalid database", 400)
				return
			}
			in.Database = &db
		}
		if raw, ok := q["cursor"]; ok {
			in.Cursor = &raw[0]
		}
		if raw, ok := q["key"]; ok {
			in.Key = &raw[0]
		}
		if raw, ok := q["key_b64"]; ok {
			in.KeyB64 = &raw[0]
		}
		out, err := a.runOperation(r.Context(), id, operation, in)
		if err != nil {
			e := err.(*apiError)
			http.Error(w, e.message, e.status)
			return
		}
		if download, ok := out.(exportResult); ok {
			w.Header().Set("Content-Type", download.ContentType)
			w.Header().Set("Content-Disposition", "attachment; filename=\"redis-key."+download.Extension+"\"")
			_, _ = w.Write(download.Body)
			return
		}
		writeJSON(w, out)
	}
}
