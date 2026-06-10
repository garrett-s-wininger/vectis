package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/encoding/protojson"
)

func DecodeDefinitionJSON(data []byte, dst *api.Job) error {
	if dst == nil {
		return fmt.Errorf("job definition target is nil")
	}

	normalized, err := normalizeSecretDeliveryTypeAliases(data)
	if err != nil {
		return err
	}

	if err := protojson.Unmarshal(normalized, dst); err != nil {
		return err
	}

	return nil
}

func normalizeSecretDeliveryTypeAliases(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var doc any
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("invalid trailing JSON data")
	}

	changed, err := normalizeJobSecretDeliveryTypes(doc)
	if err != nil {
		return nil, err
	}

	if !changed {
		return data, nil
	}

	normalized, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	return normalized, nil
}

func normalizeJobSecretDeliveryTypes(doc any) (bool, error) {
	job, ok := doc.(map[string]any)
	if !ok {
		return false, nil
	}

	secrets, ok := job["secrets"].([]any)
	if !ok {
		return false, nil
	}

	changed := false
	for i, raw := range secrets {
		secret, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		delivery, ok := secret["delivery"].(map[string]any)
		if !ok {
			continue
		}

		rawType, ok := delivery["type"].(string)
		if !ok {
			continue
		}

		value, err := secretDeliveryTypeAlias(rawType)
		if err != nil {
			return false, fmt.Errorf("secrets[%d].delivery.type: %w", i, err)
		}

		delivery["type"] = value
		changed = true
	}

	return changed, nil
}

func secretDeliveryTypeAlias(raw string) (int32, error) {
	value := strings.TrimSpace(raw)
	switch strings.ToLower(value) {
	case "file":
		return int32(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE), nil
	case "unspecified":
		return int32(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_UNSPECIFIED), nil
	}

	if enumValue, ok := api.SecretDeliveryType_value[value]; ok {
		return enumValue, nil
	}

	return 0, fmt.Errorf("unknown secret delivery type %q", raw)
}
