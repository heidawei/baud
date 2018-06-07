package bleve

import (
	"errors"
	"encoding/json"
	"github.com/blevesearch/bleve/mapping"
)

type DocumentMapping struct {
	*mapping.DocumentMapping
}

func NewDocumentMapping() *DocumentMapping {
	return &DocumentMapping{DocumentMapping: mapping.NewDocumentStaticMapping()}
}

type FieldMapping struct {
	Name     string
	*mapping.FieldMapping
}

func NewFieldMapping(name string) *FieldMapping {
	return &FieldMapping{Name: name}
}

func (f *FieldMapping)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Type     string       `json:"type"`
		Analyzer *string      `json:"analyzer,omitempty"`
		DocValues *bool       `json:"doc_values,omitempty"`
		Index     *string     `json:"index,omitempty"`
		Store     *bool       `json:"store,omitempty"`
		TermVector *string    `json:"term_vector,omitempty"`
		IncludeInAll *bool    `json:"include_in_all,omitempty"`
		Format   *string      `json:"format,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var fieldMapping *mapping.FieldMapping
	switch tmp.Type {
	case "text", "string", "keyword":
		fieldMapping = mapping.NewTextFieldMapping()
	case "date":
		fieldMapping = mapping.NewDateTimeFieldMapping()
	case "long", "integer", "short", "byte", "double", "float":
		fieldMapping = mapping.NewNumericFieldMapping()
	case "boolean":
		fieldMapping = mapping.NewBooleanFieldMapping()
	case "geo_point":
		fieldMapping = mapping.NewGeoPointFieldMapping()
	default:
		return errors.New("invalid field type")
	}
	if tmp.Store != nil {
		fieldMapping.Store = *tmp.Store
	}
	if tmp.Analyzer != nil {
		fieldMapping.Analyzer = *tmp.Analyzer
	}
	if tmp.Index != nil {
		switch *tmp.Index {
		case "analyzed":
			fieldMapping.Index = true
		case "not_analyzed":
			fieldMapping.Index = false
		default:
			return errors.New("invalid index")
		}
	}
	if tmp.DocValues != nil {
		fieldMapping.DocValues = *tmp.DocValues
	}
	if tmp.Format != nil {
		fieldMapping.DateFormat = *tmp.Format
	}
	if tmp.IncludeInAll != nil {
		fieldMapping.IncludeInAll = *tmp.IncludeInAll
	}
	if tmp.TermVector != nil {
		if *tmp.TermVector == "no" {
			fieldMapping.IncludeTermVectors = false
		} else {
			fieldMapping.IncludeTermVectors = true
		}
	}
	fieldMapping.Name = f.Name
	f.FieldMapping = fieldMapping
	return nil
}

type All struct {
	Enabled   bool     `json:"enabled"`
}

func (d *DocumentMapping)UnmarshalJSON(schema []byte) error {
	tmp := struct {
		All       *All                         `json:"_all,omitempty"`
		Properties map[string]json.RawMessage `json:"properties"`
	}{}
	err := json.Unmarshal(schema, &tmp)
    if err != nil {
    	return err
    }
    for name, data := range tmp.Properties {
    	f := NewFieldMapping(name)
    	err = json.Unmarshal(data, f)
    	if err != nil {
    		// try property
    		doc := NewDocumentMapping()
    		err = json.Unmarshal(data, doc)
    		if err != nil {
    			return err
		    }
    		d.AddSubDocumentMapping(name, doc.DocumentMapping)
		    continue
	    }
	    d.AddFieldMapping(f.FieldMapping)
    }
    return nil
}

func ParseSchema(schema []byte) ([]*mapping.DocumentMapping, error) {
	tmp := struct {
		Mapping   map[string]json.RawMessage  `json:"mappings"`
	}{Mapping: make(map[string]json.RawMessage)}

	err := json.Unmarshal(schema, &tmp)
	if err != nil {
		return nil, err
	}

	var dms []*mapping.DocumentMapping
	for _, data := range tmp.Mapping {
		dm := NewDocumentMapping()
		err = json.Unmarshal(data, dm)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dm.DocumentMapping)
	}
	return dms, nil
}
