package query

import (
	"encoding/json"
	"errors"

	"github.com/blevesearch/bleve/search/query"
)

func ParseQuery(data []byte) (query.Query, error) {
	tmp := make(map[string]json.RawMessage)
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}
	rawMessage, hasFiltered := tmp["filtered"]
	if hasFiltered {
		filter := NewFilteredQuery()
		err = json.Unmarshal([]byte(rawMessage), filter)
		if err != nil {
			return nil, err
		}
		return filter.Query, nil
	}
	rawMessage, hasBool := tmp["bool"]
	if hasBool {
		bool := NewBoolQuery()
		err = json.Unmarshal([]byte(rawMessage), bool)
		if err != nil {
			return nil, err
		}
		return bool.Query, nil
	}
	rawMessage, hasRange := tmp["range"]
	if hasRange {
		range_ := NewRangeQuery()
		err = json.Unmarshal([]byte(rawMessage), range_)
		if err != nil {
			return nil, err
		}
		return range_.Query, nil
	}
	rawMessage, hasTerm := tmp["term"]
	if hasTerm {
		term := NewTermQuery()
		err = json.Unmarshal([]byte(rawMessage), term)
		if err != nil {
			return nil, err
		}
		return term.Query, nil
	}
	rawMessage, hasTerms := tmp["terms"]
	if hasTerms {
		terms := NewTermsQuery()
		err = json.Unmarshal([]byte(rawMessage), terms)
		if err != nil {
			return nil, err
		}
		return terms.Query, nil
	}
	rawMessage, hasPrefix := tmp["prefix"]
	if hasPrefix {
		prefix := NewPrefixQuery()
		err = json.Unmarshal([]byte(rawMessage), prefix)
		if err != nil {
			return nil, err
		}
		return prefix.Query, nil
	}
	rawMessage, hasMatch := tmp["match"]
	if hasMatch {
		match := NewMatchQuery()
		err = json.Unmarshal([]byte(rawMessage), match)
		if err != nil {
			return nil, err
		}
		return match.Query, nil
	}
	rawMessage, hasMatchAll := tmp["match_all"]
	if hasMatchAll {
		matchAll := NewMatchAllQuery()
		err = json.Unmarshal([]byte(rawMessage), matchAll)
		if err != nil {
			return nil, err
		}
		return matchAll.Query, nil
	}
	rawMessage, hasWildCard := tmp["wildcard"]
	if hasWildCard {
		wildcard := NewWildcardQuery()
		err = json.Unmarshal([]byte(rawMessage), wildcard)
		if err != nil {
			return nil, err
		}
		return wildcard.Query, nil
	}
	rawMessage, hasFuzzy := tmp["fuzzy"]
	if hasFuzzy {
		fuzzy := NewFuzzyQuery()
		err = json.Unmarshal([]byte(rawMessage), fuzzy)
		if err != nil {
			return nil, err
		}
		return fuzzy, nil
	}
	rawMessage, hasConstantScore := tmp["constant_score"]
	if hasConstantScore {
		score := NewConstantScoreQuery()
		err = json.Unmarshal([]byte(rawMessage), score)
		if err != nil {
			return nil, err
		}
		return score.Query, nil
	}
	rawMessage, hasRegexp := tmp["regexp"]
	if hasRegexp {
		regexp := NewRegexpQuery()
		err = json.Unmarshal([]byte(rawMessage), regexp)
		if err != nil {
			return nil, err
		}
		return regexp.Query, nil
	}
	rawMessage, hasDisMax := tmp["dis_max"]
	if hasDisMax {
		disMax := NewDisMaxQuery()
		err = json.Unmarshal([]byte(rawMessage), disMax)
		if err != nil {
			return nil, err
		}
		return disMax.Query, nil
	}
	rawMessage, hasMultiMatch := tmp["multi_match"]
	if hasMultiMatch {
		multiMatch := NewMultiMatch()
		err = json.Unmarshal(rawMessage, multiMatch)
		if err != nil {
			return nil, err
		}
		return multiMatch.Query, nil
	}
	return nil, errors.New("invalid query")
}
