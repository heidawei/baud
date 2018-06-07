package engine

import (
	"encoding/json"
	"time"
)

type SearchRequest struct {
	//TODO
	Index string   `json:"_index""`
	Type  string   `json:"_type"`
	// default json
	ProtocolType string  `json:"-"`
	// default 10
	Size      int    `json:"size,omitempty"`
	// default 0
	From      int    `json:"from,omitempty"`
	Fields    []string `json:"fields"`
	Query     []byte `json:"query"`
	Explain   bool   `json:"explain,omitempty"`
	Timeout   time.Duration `json:"time_out,omitempty"`
	IncludeLocations bool `json:"include_locations,omitempty"`
}

func NewSearchQuery(_index, _type string) *SearchRequest {
	return &SearchRequest{Index:_index, Type:_type, ProtocolType: "json",
	Size: 10, From: 0, Explain: false, Timeout: 0, Fields: []string{"*"}}
}

func (r *SearchRequest) SetSize(size int) {
	r.Size = size
}

func (r *SearchRequest) SetFrom(from int) {
	r.From = from
}

func (r *SearchRequest) SetQuery(query []byte) {
	r.Query = query
}

func (r *SearchRequest) SetExplain(e bool) {
	r.Explain = e
}

func (r *SearchRequest) SetTimeout(t time.Duration) {
	r.Timeout = t
}

func (r *SearchRequest) SetFields(fields []string) {
	r.Fields = fields
}

func (r *SearchRequest)UnmarshalJSON(data []byte) error{
	tmp := struct {
		Size  *int `json:"size,omitempty"`
		From  *int `json:"from,omitempty"`
		Query json.RawMessage `json:"query,omitempty"`
		Explain   *bool   `json:"explain,omitempty"`
		Timeout   *time.Duration `json:"time_out,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	if tmp.From != nil {
		r.SetFrom(*tmp.From)
	}
	if tmp.Size != nil {
		r.SetSize(*tmp.Size)
	}
	r.SetQuery(tmp.Query)
	if tmp.Timeout != nil {
		r.SetTimeout(*tmp.Timeout)
	}
	if tmp.Explain != nil {
		r.SetExplain(*tmp.Explain)
	}
	return nil
}

func (r *SearchRequest)MarshalJSON() ([]byte, error) {
	tmp := struct {
		Index string   `json:"_index""`
		Type  string   `json:"_type"`
		// default 10
		Size      int    `json:"size,omitempty"`
		// default 0
		From      int    `json:"from,omitempty"`
		Query     string `json:"query"`
	}{Index: r.Index, Type: r.Type, Size: r.Size, From: r.From, Query: string(r.Query)}
	return json.Marshal(tmp)
}

func (r *SearchRequest)Parse(data []byte) (error) {
	err := json.Unmarshal(data, r)
	if err != nil {
		return err
	}
	return nil
}