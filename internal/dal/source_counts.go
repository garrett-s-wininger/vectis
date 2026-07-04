package dal

import "strings"

func normalizeSourceCountIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}

		if _, ok := seen[id]; ok {
			continue
		}

		seen[id] = struct{}{}
		out = append(out, id)
	}

	return out
}

func sourceCountDeclaredIDsCTE(ids []string) (string, []any) {
	ids = normalizeSourceCountIDs(ids)
	if len(ids) == 0 {
		return "declared_source_ids(id) AS (SELECT CAST('' AS TEXT) WHERE 1 = 0)", nil
	}

	args := make([]any, 0, len(ids))
	var b strings.Builder
	b.WriteString("declared_source_ids(id) AS (VALUES ")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString("(CAST(? AS TEXT))")
		args = append(args, id)
	}
	b.WriteByte(')')

	return b.String(), args
}
