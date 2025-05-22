package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Main configuration structure matching the JSON files
type Config struct {
	EngineConfig         EngineConfig       `json:"engineConfig"`
	Version              string             `json:"version"`
	Subgraphs            []Subgraph         `json:"subgraphs"`
	FeatureFlagConfigs   FeatureFlagConfigs `json:"featureFlagConfigs"`
	CompatibilityVersion string             `json:"compatibilityVersion"`
}

type EngineConfig struct {
	DefaultFlushInterval     string                    `json:"defaultFlushInterval"`
	DatasourceConfigurations []DatasourceConfiguration `json:"datasourceConfigurations"`
	FieldConfigurations      []FieldConfiguration      `json:"fieldConfigurations"`
	GraphqlSchema            string                    `json:"graphqlSchema"`
	StringStorage            map[string]string         `json:"stringStorage"`
}

type DatasourceConfiguration struct {
	Kind                       string        `json:"kind"`
	RootNodes                  []Node        `json:"rootNodes"`
	ChildNodes                 []Node        `json:"childNodes"`
	OverrideFieldPathFromAlias bool          `json:"overrideFieldPathFromAlias"`
	CustomGraphql              CustomGraphql `json:"customGraphql"`
	RequestTimeoutSeconds      string        `json:"requestTimeoutSeconds"`
	ID                         string        `json:"id"`
}

type Node struct {
	TypeName   string   `json:"typeName"`
	FieldNames []string `json:"fieldNames"`
}

type CustomGraphql struct {
	Fetch          FetchConfig        `json:"fetch"`
	Subscription   SubscriptionConfig `json:"subscription"`
	Federation     FederationConfig   `json:"federation"`
	UpstreamSchema UpstreamSchema     `json:"upstreamSchema"`
}

type FetchConfig struct {
	URL     VariableContent        `json:"url"`
	Method  string                 `json:"method"`
	Body    map[string]interface{} `json:"body"`
	BaseUrl map[string]interface{} `json:"baseUrl"`
	Path    map[string]interface{} `json:"path"`
}

type SubscriptionConfig struct {
	Enabled              bool            `json:"enabled"`
	URL                  VariableContent `json:"url"`
	Protocol             string          `json:"protocol"`
	WebsocketSubprotocol string          `json:"websocketSubprotocol"`
}

type FederationConfig struct {
	Enabled    bool   `json:"enabled"`
	ServiceSdl string `json:"serviceSdl"`
}

type UpstreamSchema struct {
	Key string `json:"key"`
}

type VariableContent struct {
	StaticVariableContent string `json:"staticVariableContent"`
}

type FieldConfiguration struct {
	TypeName               string                  `json:"typeName"`
	FieldName              string                  `json:"fieldName"`
	ArgumentsConfiguration []ArgumentConfiguration `json:"argumentsConfiguration"`
}

type ArgumentConfiguration struct {
	Name       string `json:"name"`
	SourceType string `json:"sourceType"`
}

type Subgraph struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	RoutingUrl string `json:"routingUrl"`
}

type FeatureFlagConfigs struct {
	ConfigByFeatureFlagName map[string]FeatureFlagConfig `json:"configByFeatureFlagName"`
}

type FeatureFlagConfig struct {
	EngineConfig EngineConfig `json:"engineConfig"`
	Version      string       `json:"version"`
	Subgraphs    []Subgraph   `json:"subgraphs"`
}

// SubgraphInput represents the input for a new subgraph
type SubgraphInput struct {
	Name       string `json:"Name"`
	SchemaPath string `json:"SchemaPath"`
	Endpoint   string `json:"Endpoint"`
	ID         string `json:"ID"`
}

// FeatureFlagInput represents input for a feature flag with its subgraphs
type FeatureFlagInput struct {
	Name      string          `json:"Name"`
	Subgraphs []SubgraphInput `json:"Subgraphs"`
}

// SchemaParser extracts root and child nodes from a GraphQL schema
func parseSchema(schemaContent string) ([]Node, []Node, []FieldConfiguration, error) {
	var rootNodes []Node
	var childNodes []Node
	fieldConfigsMap := make(map[string]FieldConfiguration)

	// Pre-process the schema to remove problematic content
	schemaContent = cleanSchemaContent(schemaContent)

	lines := strings.Split(schemaContent, "\n")
	currentType := ""
	inTypeDefinition := false
	inInterfaceDefinition := false
	inInputDefinition := false
	inEnumDefinition := false
	inScalarDefinition := false
	inUnionDefinition := false
	inDirectiveDefinition := false
	inMultilineComment := false
	inStringLiteral := false

	// Helper function to clean up field names with directives
	cleanFieldName := func(fieldName string) string {
		if atIdx := strings.Index(fieldName, "@"); atIdx != -1 {
			fieldName = strings.TrimSpace(fieldName[:atIdx])
		}
		if bracketIdx := strings.Index(fieldName, "("); bracketIdx != -1 {
			fieldName = strings.TrimSpace(fieldName[:bracketIdx])
		}
		return fieldName
	}

	// Helper function to add field to the appropriate node list
	addToNodeList := func(typeName, fieldName string, isRoot bool) {
		found := false
		var nodeList *[]Node
		if isRoot {
			nodeList = &rootNodes
		} else {
			nodeList = &childNodes
		}

		for i, node := range *nodeList {
			if node.TypeName == typeName {
				// Check for duplicate fields
				fieldExists := false
				for _, existingField := range (*nodeList)[i].FieldNames {
					if existingField == fieldName {
						fieldExists = true
						break
					}
				}
				if !fieldExists && fieldName != "" {
					(*nodeList)[i].FieldNames = append((*nodeList)[i].FieldNames, fieldName)
				}
				found = true
				break
			}
		}

		if !found && fieldName != "" && typeName != "" {
			*nodeList = append(*nodeList, Node{
				TypeName:   typeName,
				FieldNames: []string{fieldName},
			})
		}
	}

	// Helper function to parse field arguments
	parseArguments := func(argString string) []ArgumentConfiguration {
		var args []ArgumentConfiguration
		var currentArg strings.Builder
		nestedLevel := 0
		inArgString := false

		for _, char := range argString {
			switch char {
			case '"':
				inArgString = !inArgString
				currentArg.WriteRune(char)
			case '(', '[', '{':
				nestedLevel++
				currentArg.WriteRune(char)
			case ')', ']', '}':
				nestedLevel--
				currentArg.WriteRune(char)
			case ',':
				if nestedLevel == 0 && !inArgString {
					// End of argument
					argText := strings.TrimSpace(currentArg.String())
					if argText != "" {
						parts := strings.SplitN(argText, ":", 2)
						if len(parts) > 0 {
							argName := strings.TrimSpace(parts[0])
							if argName != "" {
								args = append(args, ArgumentConfiguration{
									Name:       argName,
									SourceType: "FIELD_ARGUMENT",
								})
							}
						}
					}
					currentArg.Reset()
				} else {
					currentArg.WriteRune(char)
				}
			default:
				currentArg.WriteRune(char)
			}
		}

		// Process the last argument
		argText := strings.TrimSpace(currentArg.String())
		if argText != "" {
			parts := strings.SplitN(argText, ":", 2)
			if len(parts) > 0 {
				argName := strings.TrimSpace(parts[0])
				if argName != "" {
					args = append(args, ArgumentConfiguration{
						Name:       argName,
						SourceType: "FIELD_ARGUMENT",
					})
				}
			}
		}

		return args
	}

	// Process each line of the schema
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines
		if trimmedLine == "" {
			continue
		}

		// Handle multi-line string literals
		if inStringLiteral {
			if strings.Contains(trimmedLine, "\"") && !strings.HasSuffix(trimmedLine, "\\\"") {
				inStringLiteral = false
			}
			continue
		}

		// Handle multiline comments
		if inMultilineComment {
			if strings.Contains(trimmedLine, "*/") {
				inMultilineComment = false
			}
			continue
		}

		// Skip line comments
		if strings.HasPrefix(trimmedLine, "#") || strings.HasPrefix(trimmedLine, "//") {
			continue
		}

		// Detect the start of multiline comments
		if strings.Contains(trimmedLine, "/*") {
			inMultilineComment = true
			continue
		}

		// Skip invalid content lines
		if strings.Contains(trimmedLine, "UNDEFINED") ||
			strings.Contains(trimmedLine, "columns and relationships") ||
			strings.Contains(trimmedLine, "aggregated selection") ||
			strings.Contains(strings.ToLower(trimmedLine), "input type for") {
			continue
		}

		// Detect directive definition (skip)
		if strings.HasPrefix(trimmedLine, "directive @") {
			inDirectiveDefinition = true
			continue
		}

		if inDirectiveDefinition {
			if strings.HasSuffix(trimmedLine, ")") ||
				!strings.Contains(trimmedLine, "(") {
				inDirectiveDefinition = false
			}
			continue
		}

		// Skip schema definition block
		if strings.HasPrefix(trimmedLine, "schema {") {
			bracketLevel := 1
			for j := i + 1; j < len(lines) && bracketLevel > 0; j++ {
				schemaLine := strings.TrimSpace(lines[j])
				bracketLevel += strings.Count(schemaLine, "{")
				bracketLevel -= strings.Count(schemaLine, "}")
				if bracketLevel == 0 {
					i = j // Skip past the schema block
				}
			}
			continue
		}

		// Detect type definition start
		if strings.HasPrefix(trimmedLine, "type ") && !inTypeDefinition {
			typeName := extractTypeName(trimmedLine, "type")
			if typeName != "" {
				currentType = typeName
				inTypeDefinition = true
			}
			continue
		}

		// Detect interface definition
		if strings.HasPrefix(trimmedLine, "interface ") && !inInterfaceDefinition {
			typeName := extractTypeName(trimmedLine, "interface")
			if typeName != "" {
				currentType = typeName
				inInterfaceDefinition = true
			}
			continue
		}

		// Detect input type definition
		if strings.HasPrefix(trimmedLine, "input ") && !inInputDefinition {
			typeName := extractTypeName(trimmedLine, "input")
			if typeName != "" {
				currentType = typeName
				inInputDefinition = true
			}
			continue
		}

		// Detect enum definition
		if strings.HasPrefix(trimmedLine, "enum ") && !inEnumDefinition {
			typeName := extractTypeName(trimmedLine, "enum")
			if typeName != "" {
				currentType = typeName
				inEnumDefinition = true
			}
			continue
		}

		// Detect scalar definition
		if strings.HasPrefix(trimmedLine, "scalar ") && !inScalarDefinition {
			typeName := strings.TrimSpace(strings.TrimPrefix(trimmedLine, "scalar"))
			if typeName != "" {
				currentType = typeName
				inScalarDefinition = true
				// Add scalar as child node with no fields
				addToNodeList(typeName, "", false)
				inScalarDefinition = false
				currentType = ""
			}
			continue
		}

		// Detect union definition
		if strings.HasPrefix(trimmedLine, "union ") && !inUnionDefinition {
			parts := strings.Split(trimmedLine, "=")
			if len(parts) > 0 {
				typeName := strings.TrimSpace(strings.TrimPrefix(parts[0], "union"))
				if typeName != "" {
					currentType = typeName
					inUnionDefinition = true
					// Add union as child node with no fields initially
					addToNodeList(typeName, "", false)

					// If this is a one-line union definition with types
					if len(parts) > 1 {
						unionTypes := strings.Split(parts[1], "|")
						for _, uType := range unionTypes {
							uType = strings.TrimSpace(uType)
							if uType != "" {
								// Each union type becomes a "field" of the union
								addToNodeList(typeName, uType, false)
							}
						}
						inUnionDefinition = false
						currentType = ""
					}
				}
			}
			continue
		}

		// Handle type definition end
		if (inTypeDefinition || inInterfaceDefinition || inInputDefinition ||
			inEnumDefinition || inUnionDefinition) && trimmedLine == "}" {

			inTypeDefinition = false
			inInterfaceDefinition = false
			inInputDefinition = false
			inEnumDefinition = false
			inUnionDefinition = false
			currentType = ""
			continue
		}

		// Process fields within a type definition
		if (inTypeDefinition || inInterfaceDefinition || inInputDefinition) && currentType != "" {
			// For field definitions that contain a colon (name: Type)
			if strings.Contains(trimmedLine, ":") {
				colonIdx := strings.LastIndex(trimmedLine, ":")
				if colonIdx != -1 {
					fieldPart := strings.TrimSpace(trimmedLine[:colonIdx])

					// Extract field name and arguments
					argStartIdx := strings.Index(fieldPart, "(")
					if argStartIdx != -1 {
						// Field has arguments
						fieldName := cleanFieldName(fieldPart[:argStartIdx])
						argEndIdx := strings.LastIndex(fieldPart, ")")

						if argEndIdx > argStartIdx {
							argString := fieldPart[argStartIdx+1 : argEndIdx]
							args := parseArguments(argString)

							if len(args) > 0 {
								// Create field configuration for fields with arguments
								key := currentType + "." + fieldName
								if _, exists := fieldConfigsMap[key]; !exists && fieldName != "" {
									fieldConfigsMap[key] = FieldConfiguration{
										TypeName:               currentType,
										FieldName:              fieldName,
										ArgumentsConfiguration: args,
									}
								}
							}
						}

						currentField := fieldName
						// Add to appropriate node list
						if isRootType(currentType) {
							addToNodeList(currentType, currentField, true)
						} else {
							addToNodeList(currentType, currentField, false)
						}
					} else {
						// Field without arguments
						currentField := cleanFieldName(fieldPart)
						// Add to appropriate node list
						if isRootType(currentType) {
							addToNodeList(currentType, currentField, true)
						} else {
							addToNodeList(currentType, currentField, false)
						}
					}
				}
			} else if inEnumDefinition {
				// Handle enum values (no colons)
				enumValue := cleanFieldName(trimmedLine)
				if enumValue != "" && enumValue != "{" {
					addToNodeList(currentType, enumValue, false)
				}
			} else if inUnionDefinition && strings.Contains(trimmedLine, "|") {
				// Handle multi-line union type definitions
				unionTypes := strings.Split(trimmedLine, "|")
				for _, uType := range unionTypes {
					uType = strings.TrimSpace(uType)
					if uType != "" {
						addToNodeList(currentType, uType, false)
					}
				}
			}
		}
	}

	// Convert field configurations map to slice
	fieldConfigs := make([]FieldConfiguration, 0, len(fieldConfigsMap))
	for _, v := range fieldConfigsMap {
		fieldConfigs = append(fieldConfigs, v)
	}

	// Sort field configurations for stable output
	sort.Slice(fieldConfigs, func(i, j int) bool {
		if fieldConfigs[i].TypeName == fieldConfigs[j].TypeName {
			return fieldConfigs[i].FieldName < fieldConfigs[j].FieldName
		}
		return fieldConfigs[i].TypeName < fieldConfigs[j].TypeName
	})

	return rootNodes, childNodes, fieldConfigs, nil
}

// cleanSchemaContent removes problematic content from the schema
func cleanSchemaContent(content string) string {
	// Remove quotes around UNDEFINED if they exist
	content = regexp.MustCompile(`"UNDEFINED"`).ReplaceAllString(content, "")

	// Remove any standalone UNDEFINED text
	content = regexp.MustCompile(`(?m)^.*UNDEFINED.*$`).ReplaceAllString(content, "")

	// Remove problematic description lines
	content = regexp.MustCompile(`(?m)^.*aggregated selection.*$`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?m)^.*columns and relationships.*$`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?m)^.*input type for.*$`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?m)^.*aggregate fields.*$`).ReplaceAllString(content, "")

	// Further strip out any input_type comments that don't have proper GraphQL definitions
	lines := strings.Split(content, "\n")
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip problematic lines
		if strings.Contains(trimmed, "input type") && !strings.HasPrefix(trimmed, "input ") {
			continue
		}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// Helper function to check if a type is a root type
func isRootType(typeName string) bool {
	return typeName == "Query" || typeName == "Mutation" || typeName == "Subscription"
}

// Helper function to extract type name from definition line
func extractTypeName(line string, keyword string) string {
	// Remove the keyword
	withoutKeyword := strings.TrimPrefix(line, keyword+" ")

	// Handle potential implements, directives, etc.
	parts := strings.Fields(withoutKeyword)
	if len(parts) > 0 {
		typeName := parts[0]

		// Clean up name (remove potential braces, directives)
		if braceIdx := strings.Index(typeName, "{"); braceIdx != -1 {
			typeName = typeName[:braceIdx]
		}
		if braceIdx := strings.Index(typeName, "@"); braceIdx != -1 {
			typeName = typeName[:braceIdx]
		}
		if braceIdx := strings.Index(typeName, "implements"); braceIdx != -1 {
			typeName = typeName[:braceIdx]
		}

		// Cleanup any trailing characters
		typeName = strings.TrimRight(typeName, " \t{")

		return typeName
	}
	return ""
}

// Generate a hash key for the schema
func generateSchemaKey(content string) string {
	// Create a SHA256 hash for the content
	hash := sha256.Sum256([]byte(content))
	// Convert first 20 bytes to hex string
	return fmt.Sprintf("%x", hash[:20])
}

// Create a new datasource configuration for a subgraph
func createDatasource(input SubgraphInput) (DatasourceConfiguration, map[string]string, []FieldConfiguration, error) {
	// Read schema file
	schemaContent, err := ioutil.ReadFile(input.SchemaPath)
	if err != nil {
		return DatasourceConfiguration{}, nil, nil, fmt.Errorf("failed to read schema file: %v", err)
	}

	// Pre-clean schema content before parsing
	cleanedSchema := cleanSchemaContent(string(schemaContent))

	// Parse schema to extract types
	rootNodes, childNodes, fieldConfigs, err := parseSchema(cleanedSchema)
	if err != nil {
		return DatasourceConfiguration{}, nil, nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	// Generate a schema key - using a more consistent hash generation
	schemaKey := generateSchemaKey(string(schemaContent))

	// Create the datasource configuration
	datasource := DatasourceConfiguration{
		Kind:                       "GRAPHQL",
		RootNodes:                  rootNodes,
		ChildNodes:                 childNodes,
		OverrideFieldPathFromAlias: true,
		CustomGraphql: CustomGraphql{
			Fetch: FetchConfig{
				URL: VariableContent{
					StaticVariableContent: input.Endpoint,
				},
				Method:  "POST",
				Body:    map[string]interface{}{},
				BaseUrl: map[string]interface{}{},
				Path:    map[string]interface{}{},
			},
			Subscription: SubscriptionConfig{
				Enabled: true,
				URL: VariableContent{
					StaticVariableContent: input.Endpoint,
				},
				Protocol:             "GRAPHQL_SUBSCRIPTION_PROTOCOL_WS",
				WebsocketSubprotocol: "GRAPHQL_WEBSOCKET_SUBPROTOCOL_AUTO",
			},
			Federation: FederationConfig{
				Enabled:    true,
				ServiceSdl: string(schemaContent),
			},
			UpstreamSchema: UpstreamSchema{
				Key: schemaKey,
			},
		},
		RequestTimeoutSeconds: "10",
		ID:                    input.ID,
	}

	// Create string storage entry with the consistent key
	stringStorage := map[string]string{
		schemaKey: string(schemaContent),
	}

	return datasource, stringStorage, fieldConfigs, nil
}

// Create a new feature flag configuration
func createFeatureFlag(name string, inputs []SubgraphInput) (FeatureFlagConfig, error) {
	var featureFlag FeatureFlagConfig
	var datasources []DatasourceConfiguration
	var subgraphs []Subgraph
	stringStorage := make(map[string]string)
	fieldConfigMap := make(map[string]FieldConfiguration) // <-- Use a map for deduplication

	// Process each subgraph input
	for _, input := range inputs {
		fmt.Printf("Processing subgraph '%s' from %s\n", input.Name, input.SchemaPath)
		datasource, storage, fieldConfigsFromDatasource, err := createDatasource(input)
		if err != nil {
			return FeatureFlagConfig{}, fmt.Errorf("failed to create datasource for %s: %v", input.Name, err)
		}

		datasources = append(datasources, datasource)

		// Add subgraph
		subgraphs = append(subgraphs, Subgraph{
			ID:         input.ID,
			Name:       input.Name,
			RoutingUrl: input.Endpoint,
		})

		// Merge string storage
		for k, v := range storage {
			// Validate schema before adding to storage
			if err := validateSchema(v); err != nil {
				return FeatureFlagConfig{}, fmt.Errorf("invalid schema content for subgraph %s (key: %s): %v", input.Name, k, err)
			}
			stringStorage[k] = v
		}

		// Deduplicate field configurations by typeName+fieldName
		for _, fc := range fieldConfigsFromDatasource {
			key := fc.TypeName + "." + fc.FieldName
			fieldConfigMap[key] = fc
		}
	}

	// Convert map to slice and sort for stable output
	fieldConfigurations := make([]FieldConfiguration, 0, len(fieldConfigMap))
	for _, v := range fieldConfigMap {
		fieldConfigurations = append(fieldConfigurations, v)
	}
	sort.Slice(fieldConfigurations, func(i, j int) bool {
		if fieldConfigurations[i].TypeName == fieldConfigurations[j].TypeName {
			return fieldConfigurations[i].FieldName < fieldConfigurations[j].FieldName
		}
		return fieldConfigurations[i].TypeName < fieldConfigurations[j].TypeName
	})

	// --- Combine schemas for the feature flag's engineConfig ---
	var combinedSchema strings.Builder
	var queryFields strings.Builder
	var mutationFields strings.Builder
	var subscriptionFields strings.Builder
	otherTypeDefs := make(map[string]string) // Map to store non-root type definitions (deduplicated)
	directives := make(map[string]bool)      // Map to store directives (deduplicated)

	// Track which root types actually exist
	hasQuery := false
	hasMutation := false
	hasSubscription := false

	// Add standard directives first (these are expected by Cosmo Router for federation)
	directives["directive @tag(name: String!) repeatable on ARGUMENT_DEFINITION | ENUM | ENUM_VALUE | FIELD_DEFINITION | INPUT_FIELD_DEFINITION | INPUT_OBJECT | INTERFACE | OBJECT | SCALAR | UNION"] = true
	directives["directive @extends on INTERFACE | OBJECT"] = true
	directives["directive @external on FIELD_DEFINITION | OBJECT"] = true
	directives["directive @key(fields: openfed__FieldSet!, resolvable: Boolean = true) repeatable on INTERFACE | OBJECT"] = true
	directives["directive @provides(fields: openfed__FieldSet!) on FIELD_DEFINITION"] = true
	directives["directive @requires(fields: openfed__FieldSet!) on FIELD_DEFINITION"] = true

	// Process each schema in string storage
	for _, content := range stringStorage {
		// Clean schema content before processing
		cleanedContent := cleanSchemaContent(content)

		// Extract directives from schema
		extractDirectives(cleanedContent, directives)

		// Extract type definitions and collect query/mutation/subscription fields
		extractTypesAndFields(cleanedContent, &queryFields, &mutationFields, &subscriptionFields, otherTypeDefs, &hasQuery, &hasMutation, &hasSubscription)
	}

	// --- Assemble the combined schema string ---

	// 1. Schema definition (include the root types that actually exist)
	combinedSchema.WriteString("schema {\n")
	if hasQuery {
		combinedSchema.WriteString("  query: Query\n")
	}
	if hasMutation {
		combinedSchema.WriteString("  mutation: Mutation\n")
	}
	if hasSubscription {
		combinedSchema.WriteString("  subscription: Subscription\n")
	}
	// In case no root types were found, at least include Query
	if !hasQuery && !hasMutation && !hasSubscription {
		combinedSchema.WriteString("  query: Query\n")
	}
	combinedSchema.WriteString("}\n\n")

	// 2. Directives (sorted for consistency)
	directiveList := make([]string, 0, len(directives))
	for dir := range directives {
		directiveList = append(directiveList, dir)
	}
	sort.Strings(directiveList)
	for _, dir := range directiveList {
		combinedSchema.WriteString(dir + "\n\n")
	}

	// 3. Root types with merged fields

	// Query type
	if queryFields.Len() > 0 || !hasQuery {
		combinedSchema.WriteString("type Query {\n")
		if queryFields.Len() > 0 {
			combinedSchema.WriteString(queryFields.String())
		} else {
			combinedSchema.WriteString("  _dummy: String # No query fields found in feature flag subgraphs\n")
		}
		combinedSchema.WriteString("}\n\n")
	}

	// Mutation type (if present)
	if mutationFields.Len() > 0 {
		combinedSchema.WriteString("type Mutation {\n")
		combinedSchema.WriteString(mutationFields.String())
		combinedSchema.WriteString("}\n\n")
	}

	// Subscription type (if present)
	if subscriptionFields.Len() > 0 {
		combinedSchema.WriteString("type Subscription {\n")
		combinedSchema.WriteString(subscriptionFields.String())
		combinedSchema.WriteString("}\n\n")
	}

	// 4. Other Types (sorted by name for consistency)
	otherTypeNames := make([]string, 0, len(otherTypeDefs))
	for name := range otherTypeDefs {
		otherTypeNames = append(otherTypeNames, name)
	}
	sort.Strings(otherTypeNames)
	for _, name := range otherTypeNames {
		// Validate type definition before adding
		typeDef := otherTypeDefs[name]
		if isValidTypeDef(typeDef) {
			combinedSchema.WriteString(typeDef + "\n\n")
		}
	}

	// 5. Federation Scalar (required by router)
	combinedSchema.WriteString("scalar openfed__FieldSet")

	// Create the engine config for the feature flag
	engineConfig := EngineConfig{
		DefaultFlushInterval:     "500",
		DatasourceConfigurations: datasources,
		FieldConfigurations:      fieldConfigurations,
		GraphqlSchema:            strings.TrimSpace(combinedSchema.String()),
		StringStorage:            stringStorage,
	}

	// Generate a version hash based on the schema
	version := "ff-" + generateSchemaKey(combinedSchema.String())[:8]

	featureFlag = FeatureFlagConfig{
		EngineConfig: engineConfig,
		Version:      version,
		Subgraphs:    subgraphs,
	}

	fmt.Printf("Created feature flag '%s' with version %s\n", name, version)

	return featureFlag, nil
}

// Helper function to check if a type definition is valid
func isValidTypeDef(typeDef string) bool {
	// Get the first line
	lines := strings.Split(typeDef, "\n")
	if len(lines) == 0 {
		return false
	}

	firstLine := strings.TrimSpace(lines[0])

	// Check if it starts with a valid GraphQL type definition keyword
	validPrefixes := []string{
		"type ", "interface ", "input ", "enum ", "scalar ", "union ",
	}

	for _, prefix := range validPrefixes {
		if strings.HasPrefix(firstLine, prefix) {
			return true
		}
	}

	return false
}

// Helper function to extract directives from schema
func extractDirectives(schemaContent string, directives map[string]bool) {
	lines := strings.Split(schemaContent, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Match directive definitions
		if strings.HasPrefix(trimmedLine, "directive @") &&
			!strings.Contains(trimmedLine, "@tag(") &&
			!strings.Contains(trimmedLine, "@extends") &&
			!strings.Contains(trimmedLine, "@external") &&
			!strings.Contains(trimmedLine, "@key(") &&
			!strings.Contains(trimmedLine, "@provides(") &&
			!strings.Contains(trimmedLine, "@requires(") {

			// Store the directive definition
			directives[trimmedLine] = true
		}
	}
}

// Helper function to extract types and fields from schema content
func extractTypesAndFields(schemaContent string, queryFields, mutationFields, subscriptionFields *strings.Builder,
	otherTypeDefs map[string]string, hasQuery, hasMutation, hasSubscription *bool) {

	lines := strings.Split(schemaContent, "\n")
	inTypeDef := false
	inCommentBlock := false
	currentTypeName := ""
	currentTypeDef := ""
	bracketLevel := 0

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines, comments
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") || strings.HasPrefix(trimmedLine, "//") {
			continue
		}

		// Handle multiline comments
		if inCommentBlock {
			if strings.Contains(trimmedLine, "*/") {
				inCommentBlock = false
			}
			continue
		}

		if strings.Contains(trimmedLine, "/*") {
			inCommentBlock = true
			continue
		}

		// Skip schema definition block
		if strings.HasPrefix(trimmedLine, "schema {") {
			bracketLevel = 1
			for j := i + 1; j < len(lines); j++ {
				schemaLine := strings.TrimSpace(lines[j])
				bracketLevel += strings.Count(schemaLine, "{")
				bracketLevel -= strings.Count(schemaLine, "}")
				if bracketLevel == 0 {
					i = j // Skip past the schema block
					break
				}
			}
			continue
		}

		// Skip problematic content lines
		if strings.Contains(trimmedLine, "UNDEFINED") ||
			strings.Contains(trimmedLine, "input type for") ||
			strings.Contains(trimmedLine, "columns and relationships") {
			continue
		}

		// Detect type definition start
		if !inTypeDef {
			// Check for various type definitions
			var typePrefix string
			var typeName string

			if strings.HasPrefix(trimmedLine, "type ") {
				typePrefix = "type"
				typeName = extractTypeName(trimmedLine, typePrefix)
			} else if strings.HasPrefix(trimmedLine, "interface ") {
				typePrefix = "interface"
				typeName = extractTypeName(trimmedLine, typePrefix)
			} else if strings.HasPrefix(trimmedLine, "input ") {
				typePrefix = "input"
				typeName = extractTypeName(trimmedLine, typePrefix)
			} else if strings.HasPrefix(trimmedLine, "enum ") {
				typePrefix = "enum"
				typeName = extractTypeName(trimmedLine, typePrefix)
			} else if strings.HasPrefix(trimmedLine, "scalar ") {
				// Scalar is typically a single-line definition
				typePrefix = "scalar"
				typeName = strings.TrimSpace(strings.TrimPrefix(trimmedLine, "scalar"))

				// Store scalar directly
				if typeName != "" && typeName != "openfed__FieldSet" &&
					!strings.Contains(typeName, "@") &&
					!strings.HasPrefix(typeName, "\"") {
					otherTypeDefs[typeName] = "scalar " + typeName
				}
				continue
			} else if strings.HasPrefix(trimmedLine, "union ") {
				typePrefix = "union"
				// Extract union name and possible type list
				parts := strings.Split(trimmedLine, "=")
				if len(parts) > 0 {
					typeName = strings.TrimSpace(strings.TrimPrefix(parts[0], "union"))

					// Store the complete union definition if valid
					if typeName != "" && !strings.Contains(typeName, "\"") {
						otherTypeDefs[typeName] = trimmedLine
					}
					continue
				}
			}

			// Process found type
			if typeName != "" && !strings.Contains(typeName, "\"") {
				// Skip standard federation scalar
				if typeName == "openfed__FieldSet" {
					continue
				}

				currentTypeName = typeName
				inTypeDef = true
				currentTypeDef = line + "\n"
				bracketLevel = strings.Count(trimmedLine, "{")

				// Track root types
				if currentTypeName == "Query" {
					*hasQuery = true
				} else if currentTypeName == "Mutation" {
					*hasMutation = true
				} else if currentTypeName == "Subscription" {
					*hasSubscription = true
				}

				// Handle single-line type definition (unlikely but possible)
				if strings.Contains(trimmedLine, "{") && strings.Contains(trimmedLine, "}") &&
					strings.Index(trimmedLine, "{") < strings.Index(trimmedLine, "}") {

					bracketLevel = 0 // Reset since we're done
					inTypeDef = false

					// Process the single-line type definition
					processSingleLineType(trimmedLine, currentTypeName, queryFields, mutationFields, subscriptionFields, otherTypeDefs)

					currentTypeName = ""
					currentTypeDef = ""
				}
				continue
			}
		}

		// Collecting lines inside a multi-line type definition
		if inTypeDef && currentTypeName != "" {
			currentTypeDef += line + "\n"
			bracketLevel += strings.Count(trimmedLine, "{")
			bracketLevel -= strings.Count(trimmedLine, "}")

			// Detect type definition end
			if bracketLevel <= 0 {
				// Process the complete type definition
				trimmedDef := strings.TrimSpace(currentTypeDef)

				if currentTypeName == "Query" {
					extractRootTypeFields(trimmedDef, queryFields)
				} else if currentTypeName == "Mutation" {
					extractRootTypeFields(trimmedDef, mutationFields)
				} else if currentTypeName == "Subscription" {
					extractRootTypeFields(trimmedDef, subscriptionFields)
				} else {
					// Only add if not already present
					if _, exists := otherTypeDefs[currentTypeName]; !exists {
						// Verify it's a valid type definition before adding
						if strings.Contains(trimmedDef, "{") || strings.HasPrefix(trimmedDef, "scalar") ||
							strings.HasPrefix(trimmedDef, "union") {
							otherTypeDefs[currentTypeName] = trimmedDef
						}
					}
				}

				inTypeDef = false
				currentTypeName = ""
				currentTypeDef = ""
			}
		}
	}
}

// Helper to process a single-line type definition
func processSingleLineType(line string, typeName string, queryFields, mutationFields, subscriptionFields *strings.Builder, otherTypeDefs map[string]string) {
	if typeName == "Query" {
		start := strings.Index(line, "{")
		end := strings.LastIndex(line, "}")
		if start != -1 && end != -1 && start < end {
			fields := strings.TrimSpace(line[start+1 : end])
			if fields != "" {
				queryFields.WriteString("  " + fields + "\n")
			}
		}
	} else if typeName == "Mutation" {
		start := strings.Index(line, "{")
		end := strings.LastIndex(line, "}")
		if start != -1 && end != -1 && start < end {
			fields := strings.TrimSpace(line[start+1 : end])
			if fields != "" {
				mutationFields.WriteString("  " + fields + "\n")
			}
		}
	} else if typeName == "Subscription" {
		start := strings.Index(line, "{")
		end := strings.LastIndex(line, "}")
		if start != -1 && end != -1 && start < end {
			fields := strings.TrimSpace(line[start+1 : end])
			if fields != "" {
				subscriptionFields.WriteString("  " + fields + "\n")
			}
		}
	} else if !strings.Contains(typeName, "UNDEFINED") && !strings.Contains(typeName, "\"") {
		// Only add if it's a valid type definition
		otherTypeDefs[typeName] = line
	}
}

// Helper to extract fields from root type definitions
func extractRootTypeFields(typeDef string, fieldsBuilder *strings.Builder) {
	lines := strings.Split(typeDef, "\n")
	inFieldsBlock := false

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines, comments and problematic content
		if trimmedLine == "" ||
			strings.HasPrefix(trimmedLine, "#") ||
			strings.HasPrefix(trimmedLine, "//") ||
			strings.Contains(trimmedLine, "UNDEFINED") ||
			strings.Contains(strings.ToLower(trimmedLine), "input type for") {
			continue
		}

		// Start of fields block
		if strings.Contains(trimmedLine, "{") {
			inFieldsBlock = true
			continue
		}

		// End of fields block
		if strings.Contains(trimmedLine, "}") {
			break
		}

		// Process field line
		if inFieldsBlock && !strings.HasPrefix(trimmedLine, "}") && !strings.HasPrefix(trimmedLine, "{") {
			fieldsBuilder.WriteString("  " + trimmedLine + "\n")
		}
	}
}

// ValidateSchema validates that the schema content is well-formed enough for basic parsing
func validateSchema(schemaContent string) error {
	// Clean the content first
	schemaContent = cleanSchemaContent(schemaContent)

	// Perform balance check on brackets
	bracketCount := 0
	inString := false
	inComment := false
	inMultilineComment := false

	lines := strings.Split(schemaContent, "\n")

	for i, line := range lines {
		// Reset line-specific flags
		inComment = false

		// Process each character
		for j := 0; j < len(line); j++ {
			// Skip out-of-bounds check
			if j >= len(line) {
				continue
			}

			// Handle comments
			if j < len(line)-1 && line[j] == '/' && line[j+1] == '/' && !inString && !inMultilineComment {
				inComment = true
				break // Skip rest of the line
			}

			if j < len(line)-1 && line[j] == '/' && line[j+1] == '*' && !inString && !inComment {
				inMultilineComment = true
			}

			if j > 0 && j < len(line) && line[j-1] == '*' && line[j] == '/' && inMultilineComment && !inString {
				inMultilineComment = false
				continue
			}

			// Skip processing in comments
			if inComment || inMultilineComment {
				continue
			}

			// Handle string literals
			if j < len(line) && line[j] == '"' {
				// Check if it's escaped
				if j > 0 && line[j-1] == '\\' {
					continue
				}
				inString = !inString
				continue
			}

			// Only count brackets outside of strings
			if !inString && j < len(line) {
				if line[j] == '{' {
					bracketCount++
				} else if line[j] == '}' {
					bracketCount--
				}

				// Check for imbalance
				if bracketCount < 0 {
					return fmt.Errorf("unbalanced brackets in schema - unexpected '}' on line %d: %s", i+1, line)
				}
			}
		}
	}

	if bracketCount != 0 {
		return fmt.Errorf("unbalanced brackets in schema - %d unclosed '{'", bracketCount)
	}

	return nil
}

func main() {
	// Define command line flags
	configFile := flag.String("config", "no-flag.json", "Path to the configuration file")
	flagName := flag.String("flag-name", "", "Name of the feature flag to add")
	schemasDir := flag.String("schemas-dir", "", "Directory containing schema files")
	subgraphsJSON := flag.String("subgraphs-json", "", "JSON array of subgraph definitions")
	flagsJSON := flag.String("flags-json", "", "JSON array of feature flag definitions")
	flag.Parse()

	// Read existing config file
	data, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// Parse existing config
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	// Initialize feature flag configs map if it doesn't exist
	if config.FeatureFlagConfigs.ConfigByFeatureFlagName == nil {
		config.FeatureFlagConfigs.ConfigByFeatureFlagName = make(map[string]FeatureFlagConfig)
	}

	// Process feature flags
	if *flagsJSON != "" {
		// Process multiple feature flags from JSON
		var flagInputs []FeatureFlagInput
		if err := json.Unmarshal([]byte(*flagsJSON), &flagInputs); err != nil {
			log.Fatalf("Failed to parse feature flags JSON: %v", err)
		}

		fmt.Printf("Processing %d feature flags from JSON input\n", len(flagInputs))

		for _, flagInput := range flagInputs {
			featureFlag, err := createFeatureFlag(flagInput.Name, flagInput.Subgraphs)
			if err != nil {
				log.Fatalf("Failed to create feature flag %s: %v", flagInput.Name, err)
			}

			config.FeatureFlagConfigs.ConfigByFeatureFlagName[flagInput.Name] = featureFlag
			fmt.Printf("Added feature flag: %s\n", flagInput.Name)
		}
	} else if *subgraphsJSON != "" {
		// Process single feature flag with subgraphs from JSON
		if *flagName == "" {
			log.Fatalf("Error: flag-name is required when using subgraphs-json")
		}

		var subgraphInputs []SubgraphInput
		if err := json.Unmarshal([]byte(*subgraphsJSON), &subgraphInputs); err != nil {
			log.Fatalf("Failed to parse subgraphs JSON: %v", err)
		}

		fmt.Printf("Processing feature flag '%s' with %d subgraphs from JSON input\n", *flagName, len(subgraphInputs))

		featureFlag, err := createFeatureFlag(*flagName, subgraphInputs)
		if err != nil {
			log.Fatalf("Failed to create feature flag: %v", err)
		}

		config.FeatureFlagConfigs.ConfigByFeatureFlagName[*flagName] = featureFlag
		fmt.Printf("Added feature flag: %s\n", *flagName)
	} else if *flagName != "" {
		// Legacy directory-based subgraph collection
		if *schemasDir == "" {
			log.Fatalf("Error: schemas-dir is required when using flag-name without subgraphs-json")
		}

		subgraphInputs := []SubgraphInput{}

		// Read schema files from directory
		files, err := ioutil.ReadDir(*schemasDir)
		if err != nil {
			log.Fatalf("Failed to read schemas directory: %v", err)
		}

		for _, file := range files {
			if !file.IsDir() && (strings.HasSuffix(file.Name(), ".graphql") || strings.HasSuffix(file.Name(), ".gql")) {
				subgraphName := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))

				// Generate a stable UUID based on the filename
				id := fmt.Sprintf("%x", sha256.Sum256([]byte(subgraphName)))[:36]
				id = id[:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:32]

				subgraphInputs = append(subgraphInputs, SubgraphInput{
					Name:       subgraphName,
					SchemaPath: filepath.Join(*schemasDir, file.Name()),
					Endpoint:   fmt.Sprintf("https://example.com/graphql/%s", subgraphName),
					ID:         id,
				})
			}
		}

		if len(subgraphInputs) == 0 {
			log.Fatalf("No valid schema files found in directory: %s", *schemasDir)
		}

		fmt.Printf("Processing feature flag '%s' with %d subgraphs from directory %s\n",
			*flagName, len(subgraphInputs), *schemasDir)

		featureFlag, err := createFeatureFlag(*flagName, subgraphInputs)
		if err != nil {
			log.Fatalf("Failed to create feature flag: %v", err)
		}

		config.FeatureFlagConfigs.ConfigByFeatureFlagName[*flagName] = featureFlag
		fmt.Printf("Added feature flag: %s\n", *flagName)
	} else {
		// No valid input method specified
		fmt.Println("Error: No valid input method specified. Use one of:")
		fmt.Println("  --flag-name with --schemas-dir")
		fmt.Println("  --flag-name with --subgraphs-json")
		fmt.Println("  --flags-json")
		flag.Usage()
		os.Exit(1)
	}

	// Write updated config back to file
	updatedData, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		log.Fatalf("Failed to marshal updated config: %v", err)
	}

	if err := ioutil.WriteFile(*configFile, updatedData, 0644); err != nil {
		log.Fatalf("Failed to write updated config file: %v", err)
	}

	fmt.Printf("Successfully updated %s with feature flag configuration(s)\n", *configFile)
}
