import json

def save_charts_to_json(results: list, output_file: str = "output.json"):
    """
    Saves the chart results to a JSON file.

    Args:
        results (list): A list of dictionaries, each containing a goal and a base64-encoded chart.
        output_file (str): Path to the output JSON file.
    """
    with open(output_file, "w") as f:
        json.dump(results, f)
    print(f"Saved {len(results)} charts to {output_file}")