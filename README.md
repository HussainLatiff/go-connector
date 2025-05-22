# go-connector
The go connector is just the main.go file. The other files are there to help with testing :)

The guide to use it is in the docs, but also just run this command
1. For a single service
```
go run main.go --config no-flag.json --flag-name spacex-flag --subgraphs-json '[{"Name":"spacex","SchemaPath":"spacex_schema.graphql","Endpoint":"https://spacex-production.up.railway.app/","ID":"6d731f0e-191a-44cd-a455-08b97766446d"}]'
```
2. For a federated service
```
go run main.go --config no-flag.json --flag-name country-character-flag --subgraphs-json '[{"Name":"countries","SchemaPath":"countries_schema.graphql","Endpoint":"https://countries.trevorblades.com/graphql","ID":"c944b5cd-0b58-4a72-a9ba-7e46715d164b"},{"Name":"rickandmorty","SchemaPath":"rickandmorty_schema.graphql","Endpoint":"https://rickandmortyapi.com/graphql","ID":"78d9618c-d48d-4ec1-be5c-c0251121f9ee"}]'
```
