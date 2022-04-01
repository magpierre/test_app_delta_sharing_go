package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

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
		df, err := delta_sharing.LoadAsDataFrame(*profile_path)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print("Table returned:\n", df.Table())
	}

	if *profile != "" {
		fmt.Println("List tables...")
		y, err := delta_sharing.NewSharingClient(context.Background(), *profile)
		if err != nil {
			log.Fatal(err)
		}
		shares, err := y.ListShares()
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range shares {
			schemas, err := y.ListSchemas(v)
			if err != nil {
				log.Fatal(err)
			}
			for _, v2 := range schemas {
				tables, err := y.ListTables(v2)
				if err != nil {
					log.Fatal(err)
				}
				for i, v := range tables {
					fmt.Println("Pos", i, "Table Name:", v.Name, "Schema:", v.Schema, "Share:", v.Share)
					r, err := y.RestClient.ListFilesInTable(v)
					if err != nil {
						log.Fatal(err)
					}
					x, err := json.MarshalIndent(r, "", "    ")
					if err != nil {
						fmt.Println(err)
					}
					fmt.Printf("response: %+v\n", string(x))

				}

			}
		}
	}
}
