package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	delta_sharing "github.com/delta-io/delta_sharing_go"
)

func main() {

	profile := flag.String("profile", "", "Path to profile for the share you would like to access")
	tablepath := flag.String("table_path", "", "The path to the table in the form <share>.<schema>.<table>")
	profile_path := flag.String("profile_path", "", "Profile path in the form: <profile_path>#<table_path>")

	flag.Parse()

	fmt.Printf("profile: %v\n", *profile)
	fmt.Printf("tablepath: %v\n", *tablepath)
	fmt.Printf("profile_path: %v\n", *profile_path)
	if profile_path != nil && *profile_path != "" {
		df := delta_sharing.LoadAsDataFrame(*profile_path)
		fmt.Print("Table returned:\n", df.Table())
	}

	if *profile != "" {
		fmt.Println("List tables...")
		y := delta_sharing.NewSharingClient(context.Background(), *profile)
		shares := y.ListShares()
		schemas := y.ListSchemas(shares[0])
		tables := y.ListTables(schemas[0])
		for i, v := range tables {
			fmt.Println("Pos", i, "Table Name:", v.Name, "Schema:", v.Schema, "Share:", v.Share)
			r := y.RestClient.ListFilesInTable(v)
			x, err := json.MarshalIndent(r, "", "    ")
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("response: %+v\n", string(x))
		}

	}
}
