import pandas as pd
from ydata_profiling import ProfileReport

def generate_profile_report(dataset_path, output_path="profile_report.html"):
    """
    Generates a data profiling report using ydata-profiling.

    Args:
        dataset_path (str): Path to the dataset file (e.g., CSV, Excel).
        output_path (str, optional): Path to save the HTML report. 
                                      Defaults to "profile_report.html".
    """
    try:
        # Load the dataset
        df = pd.read_csv(dataset_path)  # Replace with appropriate reader for other formats (e.g., pd.read_excel)

        # Generate the profiling report
        profile = ProfileReport(df, title="Dataset Profiling Report")

        # Save the report to an HTML file
        profile.to_file(output_path)
        print(f"Profiling report saved to {output_path}")

    except FileNotFoundError:
        print(f"Error: Dataset file not found at {dataset_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Example usage:
dataset_file = "attendances_export_flat.csv" # Replace with the actual path to your dataset
generate_profile_report(dataset_file)