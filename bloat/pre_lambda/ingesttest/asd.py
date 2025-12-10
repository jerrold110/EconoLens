import csv
import json

def write_test_files():
    # choose file base name
    base = "mydata"
    csv_filename = f"{base}.csv"
    meta_filename = f"{csv_filename}.metadata.json"

    # define your columns
    # Let’s say:
    #   “Text” → content field
    #   “Author” → metadata field
    #   “Category” → metadata field
    fieldnames = ["Text", "Author", "Category"]

    # sample rows
    rows = [
        {
            "Text": "The designer of polarizinglight3m is Alice.",
            "Author": "Alice",
            "Category": "News"
        },
        {
            "Text": "The color of polarizinglight3m is Brown.",
            "Author": "Bob",
            "Category": "Blog"
        }
    ]

    # write CSV file
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Wrote CSV file: {csv_filename}")

    # prepare metadata JSON
    metadata = {
        "metadataAttributes": {
            # these are overall metadata attributes (optional)
            "source": "unit-test",
            "version": "1.0"
        },
        "documentStructureConfiguration": {
            "type": "RECORD_BASED_STRUCTURE_METADATA",
            "recordBasedStructureMetadata": {
                "contentFields": [
                    {
                        "fieldName": "Text"
                    }
                ],
                "metadataFieldsSpecification": {
                    "fieldsToInclude": [
                        {
                            "fieldName": "Author"
                        },
                        {
                            "fieldName": "Category"
                        }
                    ],
                    "fieldsToExclude": []
                }
            }
        }
    }

    with open(meta_filename, mode="w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    print(f"Wrote metadata JSON file: {meta_filename}")

if __name__ == "__main__":
    write_test_files()
