package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/bndr/gotabulate"
	delta_sharing "github.com/delta-io/delta_sharing_go"
)

func main() {
	prof := flag.String("profile", "", "Path to profile for the share you would like to access")
	profile_path := flag.String("profile_path", "", "Profile path in the form: <profile_path>#<table_path>")
	flag.Parse()
	if profile_path != nil && *profile_path != "" {
		x, _ := delta_sharing.LoadAsArrowTable(*profile_path, 0)
		if x == nil {
			log.Fatal("No arrow table")
		}
		at := x.(array.Table)
		fmt.Println("Arrow", at.Schema())
		tr := array.NewTableReader(at, 100)
		tr.Retain()
		fmt.Println("")
		fmt.Println("Data:")
		var tbl [][]interface{}
		h := []string{}
		for i := 0; i < int(at.NumCols()); i++ {
			h = append(h, at.Column(i).Name())
		}

		for tr.Next() {
			rec := tr.Record()
			for pos := 0; pos < int(rec.NumRows()); pos++ {
				var d []interface{}
				for _, col := range rec.Columns() {
					switch col.DataType().ID() {
					case arrow.STRING:
						a := col.(*array.String)
						d = append(d, a.Value(pos))
					case arrow.INT16:
						i16 := col.(*array.Int16)
						d = append(d, int(i16.Value(pos)))
					case arrow.INT32:
						i32 := col.(*array.Int32)
						d = append(d, int(i32.Value(pos)))
					case arrow.INT64:
						i64 := col.(*array.Int64)
						d = append(d, int(i64.Value(pos)))
					case arrow.FLOAT16:
						f16 := col.(*array.Float16)
						d = append(d, f16.Value(pos))
					case arrow.FLOAT32:
						f32 := col.(*array.Float32)
						d = append(d, f32.Value(pos))
					case arrow.FLOAT64:
						f64 := col.(*array.Float64)
						d = append(d, f64.Value(pos))
					case arrow.BOOL:
						b := col.(*array.Boolean)
						d = append(d, b.Value(pos))
					case arrow.BINARY:
						bi := col.(*array.Binary)
						d = append(d, bi.Value(pos))
					case arrow.DATE32:
						d32 := col.(*array.Date32)
						d = append(d, d32.Value(pos))
					case arrow.DATE64:
						d64 := col.(*array.Date64)
						d = append(d, d64.Value(pos))
					case arrow.DECIMAL128:
						dec := col.(*array.Decimal128)
						d = append(d, dec.Value(pos))
					case arrow.INTERVAL_DAY_TIME:
						idt := col.(*array.DayTimeInterval)
						d = append(d, idt.Value(pos))
					}
				}
				tbl = append(tbl, d)

			}
		}
		tr.Release()
		f := gotabulate.Create(tbl)
		f.SetHeaders(h)
		f.SetAlign("left")
		f.SetEmptyString("None")
		f.SetWrapStrings(true)
		f.SetMaxCellSize(25)
		fmt.Println(f.Render("grid"))
	}

	/* Returning metadata from the Delta Sharing server */
	if *prof != "" {
		fmt.Println("List tables...")
		y, err := delta_sharing.NewSharingClient(context.Background(), *prof)
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
					fmt.Printf("Pos %d,table path %s.%s.%s\n", i, v.Share, v.Schema, v.Name)
					r, err := y.RestClient.ListFilesInTable(v)
					if err != nil {
						log.Fatal(err)
					}

					val, err := r.Metadata.GetSparkSchema()
					if err != nil {
						log.Fatal(err)
					}

					for _, f := range val.Fields {
						fmt.Printf("Schema Field:%s|%s%s|%s%t\n", strings.ToUpper(f.Name), "Datatype:", f.Type, "Nullable:", f.Nullable)
					}
					fmt.Println("Stored statistics:")
					for _, v := range r.AddFiles {
						s, err := v.GetStats()
						if err == nil {
							fmt.Println("Number of records", s.NumRecords)
							for k, value := range s.MinValues {
								fmt.Println(k, "|", value, "|", s.MaxValues[k], "|", s.NullCount[k])
							}
						} else {
							fmt.Printf("No stats available for file with id: %s\n", v.Id)
						}

					}

				}

			}
		}
	}
}
