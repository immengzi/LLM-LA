import os
import json
import glob
import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def load_data(data_dir: str) -> pd.DataFrame:
    """
    Scans the data directory, parses folder/file names, and loads JSON metrics
    into a single pandas DataFrame.
    """
    all_data = []
    
    # Use glob to find all .json files in the subdirectories
    # e.g., data_dir/prefix_identical_qwen1.7B/10.json
    json_files = glob.glob(os.path.join(data_dir, "*", "*.json"))
    
    if not json_files:
        print(f"Error: No .json files found in subdirectories of '{data_dir}'")
        print("Expected structure: data_dir/prefix_type_model/prompt_length.json")
        return pd.DataFrame()

    print(f"Found {len(json_files)} JSON files to analyze...")

    for f_path in json_files:
        try:
            # --- 1. Parse Metadata from Path ---
            
            # Get folder name (e.g., "prefix_identical_qwen1.7B")
            folder_name = os.path.basename(os.path.dirname(f_path))
            
            # Get filename (e.g., "10.json")
            filename = os.path.basename(f_path)
            
            # Extract prompt length from filename (e.g., 10)
            prompt_length = int(os.path.splitext(filename)[0])
            
            # Extract Prefix Type
            prefix_type = "Identical" if "identical" in folder_name.lower() else "Random"
            
            # Extract Model
            if "qwen" in folder_name.lower():
                model_name = "Qwen 1.7B"
            elif "ds7b" in folder_name.lower():
                model_name = "DeepSeek 7B"
            else:
                model_name = "Unknown"

            # --- 2. Load Data from JSON ---
            with open(f_path, 'r') as f:
                data = json.load(f)
            
            # Data is a list, likely with one entry per your script
            if not data:
                print(f"Warning: Skipping empty JSON file: {f_path}")
                continue
                
            record = data[0] # Get the first result from the list

            # --- 3. Append to our data list ---
            all_data.append({
                "Prompt Length": prompt_length,
                "Prefix Type": prefix_type,
                "Model": model_name,
                "P90 Latency (s)": float(record["P90 Latency (s)"]),
                "P99 Latency (s)": float(record["P99 Latency (s)"]),
                "Hit Rate (%)": float(record.get("Hit Rate (%)", 0.0)) # Use .get for safety
            })
            
        except Exception as e:
            print(f"Error processing file {f_path}: {e}")
            
    return pd.DataFrame(all_data)
def plot_latency_grid(df: pd.DataFrame, output_dir: str):
    """
    Plots Latency vs. Prompt Length, grid-separated by Latency Type (P90/P99)
    and Prefix Type (Cache Hit/Miss).
    """
    print("\nGenerating Plot 1: Latency vs. Prompt Length (Grid)")
    
    # "Melt" the DataFrame to plot P90 and P99 together
    df_melted = df.melt(
        id_vars=["Prompt Length", "Prefix Type", "Model"],
        value_vars=["P90 Latency (s)", "P99 Latency (s)"],
        var_name="Latency Type",
        value_name="Latency (s)"
    )
    
    # Create a 2x2 grid:
    # Rows: P90 vs P99
    # Cols: Identical vs Random
    g = sns.catplot(
        data=df_melted,
        x="Prompt Length",
        y="Latency (s)",
        hue="Model",          # Different colored lines for models
        col="Prefix Type",    # Separate plots for Identical vs Random
        row="Latency Type",   # Separate plots for P90 vs P99
        kind="point",         # Use a point/line plot
        height=4,
        aspect=1.2,
        legend="auto"
    )
    
    # --- FONT SIZE MODIFICATIONS START ---
    
    # 1. Set the main title font size
    g.fig.suptitle("Latency vs. Prompt Length by Model and Cache Type", y=1.03, fontsize=16) 
    
    # 2. Set the axis label font size (for the main X and Y labels)
    g.set_axis_labels("Prompt Length (Tokens, Categorical)", "Latency (s)", size=12) 
    
    # 3. Set the subplot title font size (e.g., "Identical Prefixes | P90 Latency (s)")
    g.set_titles("{col_name} Prefixes | {row_name}", size=11) 
    
    # --- THIS IS THE FIX ---
    # 4. Set the tick label font size (the numbers on the axes)
    # We iterate through all subplots (axes) in the grid
    for ax in g.axes.flat:
        # Set font size for x and y ticks without overwriting the labels
        # This will keep your "10", "1000", "10000" labels visible
        ax.tick_params(axis='both', which='major', labelsize=10)
    # --- END OF FIX ---
    
    # 5. Set the legend font sizes
    if g.legend: # Check if legend exists
        g.legend.set_title("Model", prop={'size': 11}) # Pass 'size' inside a 'prop' dict
        
        # This line was correct, but just for reference
        plt.setp(g.legend.get_texts(), fontsize=10) # Set legend item font

    # --- FONT SIZE MODIFICATIONS END ---
    
    # Save the figure
    output_path = os.path.join(output_dir, "1_latency_vs_prompt_length_grid.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved plot to {output_path}")
    plt.close()



def plot_comparison_bars(df: pd.DataFrame, output_dir: str):
    """
    Plots a grouped bar chart comparing latencies at the *longest* prompt length.
    """
    print("\nGenerating Plot 2: Latency Comparison at Max Prompt Length (Bar)")
    
    # Find the longest prompt length in the dataset
    max_prompt_length = df["Prompt Length"].max()
    df_long = df[df["Prompt Length"] == max_prompt_length]
    
    if df_long.empty:
        print("Could not generate bar plot: No data at max prompt length.")
        return

    # Melt for P90/P99 plotting
    df_melted = df_long.melt(
        id_vars=["Prefix Type", "Model"],
        value_vars=["P90 Latency (s)", "P99 Latency (s)"],
        var_name="Latency Type",
        value_name="Latency (s)"
    )
    
    g = sns.catplot(
        data=df_melted,
        x="Model",
        y="Latency (s)",
        hue="Prefix Type",    # Grouped bars for Identical vs Random
        col="Latency Type",   # Separate plots for P90 vs P99
        kind="bar",
        height=5,
        aspect=1.1,
        palette="muted"
    )
    
    g.fig.suptitle(f"Latency Comparison at {max_prompt_length} Token Prompt", y=1.03)
    g.set_axis_labels("Model", "Latency (s)")
    g.set_titles("{col_name}")
    
    output_path = os.path.join(output_dir, "2_max_prompt_latency_comparison_bar.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved plot to {output_path}")
    plt.close()

def plot_hit_rate_check(df: pd.DataFrame, output_dir: str):
    """
    Plots a bar chart of Hit Rates to confirm Identical vs Random behavior.
    """
    print("\nGenerating Plot 3: Hit Rate Sanity Check (Bar)")
    
    g = sns.catplot(
        data=df,
        x="Prompt Length",
        y="Hit Rate (%)",
        hue="Model",
        col="Prefix Type",
        kind="bar",
        height=4,
        aspect=1.2,
        palette="pastel"
    )
    
    g.fig.suptitle("Cache Hit Rate (Sanity Check)", y=1.03)
    g.set_axis_labels("Prompt Length", "Hit Rate (%)")
    g.set_titles("{col_name} Prefixes")
    
    output_path = os.path.join(output_dir, "3_hit_rate_sanity_check.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved plot to {output_path}")
    plt.close()

def plot_latency_trends_per_model(df: pd.DataFrame, output_dir: str):
    """
    Plots Latency vs. Prompt Length, separated by Model.
    Each plot shows the trend for Cache Hit (Identical) vs. Cache Miss (Random).
    """
    print("\nGenerating Plot 4: Latency Trend by Model (Hit vs. Miss)")

    # "Melt" the DataFrame to plot P90 and P99 together
    df_melted = df.melt(
        id_vars=["Prompt Length", "Prefix Type", "Model"],
        value_vars=["P90 Latency (s)", "P99 Latency (s)"],
        var_name="Latency Type",
        value_name="Latency (s)"
    )

    # Create a 2x2 grid:
    # Rows: P90 vs P99
    # Cols: Qwen vs DeepSeek
    g = sns.catplot(
        data=df_melted,
        x="Prompt Length",
        y="Latency (s)",
        hue="Prefix Type",    # This is the key comparison: Hit vs. Miss
        col="Model",          # "for each model"
        row="Latency Type",   # Separate P90 and P99
        kind="point",         # Use a point/line plot to show trend
        height=4.5,
        aspect=1.1,
        palette="deep",
        legend="auto"
    )

    g.fig.suptitle("Latency Trend vs. Prompt Length (Cache Hit vs. Miss)", y=1.03)
    g.set_axis_labels("Prompt Length (Categorical)", "Latency (s)")
    g.set_titles("{col_name} | {row_name}")
    
    # Get the legend and move it to a better position (optional)
    g.legend.set_title("Prefix Type")
    
    # Save the figure
    output_path = os.path.join(output_dir, "4_latency_trends_per_model.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved plot to {output_path}")
    plt.close()

def main():
    # --- Setup Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Analyze vLLM latency data and generate plots."
    )
    parser.add_argument(
        "data_dir",
        type=str,
        help="The root directory containing the data folders (e.g., './my_data')"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="analysis_plots",
        help="The directory where plots will be saved (default: 'analysis_plots')"
    )
    args = parser.parse_args()

    # --- Create output directory ---
    os.makedirs(args.output_dir, exist_ok=True)
    
    # --- Set Plotting Style ---
    # Using "notebook" context for slightly smaller fonts than "talk"
    sns.set_theme(style="whitegrid", context="notebook") 

    # --- 1. Load Data ---
    df = load_data(args.data_dir)
    
    if df.empty:
        print("No data loaded. Exiting.")
        return
        
    # Sort for cleaner plots
    df = df.sort_values(by=["Model", "Prefix Type", "Prompt Length"])
    
    print("\n--- Data Loaded Successfully ---")
    print(df.to_string())
    print("--------------------------------\n")

    # --- 2. Generate Plots ---
    plot_latency_grid(df, args.output_dir)
    plot_comparison_bars(df, args.output_dir)
    plot_hit_rate_check(df, args.output_dir)
    plot_latency_trends_per_model(df, args.output_dir)
    
    print("\nAnalysis complete. Plots saved to folder:", args.Toutput_dir)

if __name__ == "__main__":
    main()