import os
import json
from datetime import timedelta
from typing import List, Dict
import pandas as pd
import plotly.express as px
import nbformat
from nbformat.v4 import new_notebook, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor
from github import Github, GithubException, InputFileContent
from dagster import asset, AssetExecutionContext
from dotenv import load_dotenv
from io import StringIO
from IPython.display import display, HTML
import plotly.io as pio
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Load environment variables from a .env file if present
load_dotenv()

# Securely handle the GitHub access token
ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("GITHUB_ACCESS_TOKEN environment variable not set")


# Define the notebook template in two parts
NOTEBOOK_TEMPLATE_START = '''
import pandas as pd
import plotly.express as px
from IPython.display import display, HTML
import plotly.io as pio

# The data will be injected here as a pandas DataFrame
data = '''

NOTEBOOK_TEMPLATE_END = '''

# Ensure proper datetime conversion
data['week'] = pd.to_datetime(data['week'])
print("Loaded DataFrame Columns:", data.columns.tolist())
print("Loaded DataFrame Head:\\n", data.head())

# Group by week to ensure we don't have duplicate dates
weekly_data = data.groupby('week')['count'].sum().reset_index()
weekly_data['cumulative_stars'] = weekly_data['count'].cumsum()
weekly_data['month'] = weekly_data['week'].dt.to_period('M')
monthly_data = weekly_data.groupby('month')['count'].sum().reset_index()

# Create an interactive dashboard
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Create three subplots
fig = make_subplots(
    rows=3, 
    cols=1,
    subplot_titles=(
        'Monthly Star Growth Heatmap',
        'Star Acquisition Trend',
        'Monthly Distribution'
    ),
    vertical_spacing=0.1,
    specs=[[{"type": "heatmap"}],
           [{"type": "scatter"}],
           [{"type": "violin"}]]
)

# 1. Heatmap of stars by month and year
weekly_data['year'] = weekly_data['week'].dt.year
weekly_data['month_num'] = weekly_data['week'].dt.month
heatmap_data = weekly_data.pivot_table(
    values='count',
    index='year',
    columns='month_num',
    aggfunc='sum',
    fill_value=0
)

fig.add_trace(
    go.Heatmap(
        z=heatmap_data.values,
        x=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        y=heatmap_data.index,
        colorscale='Viridis',
        name='Monthly Stars'
    ),
    row=1, col=1
)

# 2. Trend line with moving average
weekly_data['MA7'] = weekly_data['count'].rolling(window=7).mean()
fig.add_trace(
    go.Scatter(
        x=weekly_data['week'],
        y=weekly_data['count'],
        mode='markers',
        name='Weekly Stars',
        marker=dict(size=6, color='rgba(255, 182, 193, 0.7)')
    ),
    row=2, col=1
)

fig.add_trace(
    go.Scatter(
        x=weekly_data['week'],
        y=weekly_data['MA7'],
        mode='lines',
        name='7-week Moving Average',
        line=dict(color='rgba(255, 0, 0, 0.8)', width=2)
    ),
    row=2, col=1
)

# 3. Violin plot for monthly distribution
fig.add_trace(
    go.Violin(
        y=weekly_data['count'],
        name='Star Distribution',
        box_visible=True,
        meanline_visible=True,
        fillcolor='lightseagreen',
        line_color='darkgreen'
    ),
    row=3, col=1
)

# Update layout
fig.update_layout(
    height=1200,
    width=1000,
    showlegend=True,
    template='plotly_dark',
    title_text="Dagster GitHub Stars Analysis - Advanced Visualization",
    title_x=0.5,
)

# Update axes labels
fig.update_xaxes(title_text="Month", row=1, col=1)
fig.update_yaxes(title_text="Year", row=1, col=1)
fig.update_xaxes(title_text="Date", row=2, col=1)
fig.update_yaxes(title_text="Star Count", row=2, col=1)
fig.update_xaxes(title_text="", row=3, col=1)
fig.update_yaxes(title_text="Stars Distribution", row=3, col=1)

# Force static image rendering in addition to interactive plot
import plotly.io as pio
pio.write_image(fig, 'temp_plot.png')
from IPython.display import Image
display(Image('temp_plot.png'))

# Also display interactive version
fig.show()

# Calculate and display statistics
print("\\nDetailed Statistics:")
print(f"Total stars: {weekly_data['count'].sum():,}")
print(f"Average stars per week: {weekly_data['count'].mean():.1f}")
print(f"Median stars per week: {weekly_data['count'].median():.1f}")
print(f"Highest stars in a week: {weekly_data['count'].max()}")
print(f"Total weeks tracked: {len(weekly_data)}")
print(f"Most active month: {monthly_data.loc[monthly_data['count'].idxmax(), 'month']}")
'''


@asset
def github_stargazers() -> pd.DataFrame:
    """
    Fetches the stargazers of the 'dagster-io/dagster' repository along with the date they starred the repo.

    Returns:
        pd.DataFrame: A DataFrame containing 'user' and 'starred_at' columns.
    """
    try:
        github_client = Github(ACCESS_TOKEN)
        repo = github_client.get_repo("dagster-io/dagster")
        stargazers = list(repo.get_stargazers_with_dates())
    except GithubException as e:
        raise RuntimeError(f"GitHub API error: {e.data.get('message', str(e))}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {str(e)}") from e

    # Convert to DataFrame
    data = [
        {"user": stargazer.user.login, "starred_at": stargazer.starred_at}
        for stargazer in stargazers
    ]
    df = pd.DataFrame(data)   
    return df


@asset
def github_stargazers_by_week(github_stargazers: pd.DataFrame) -> pd.DataFrame:
    github_stargazers["starred_at"] = pd.to_datetime(github_stargazers["starred_at"], errors='coerce')
    # Drop rows with NaT in 'starred_at'
    github_stargazers = github_stargazers.dropna(subset=["starred_at"])
    print(f'keys:', github_stargazers.keys())
    github_stargazers["week"] = github_stargazers["starred_at"].apply(
        lambda x: x - timedelta(days=x.weekday())  # Start of the week (Monday)
    )
    df = (
        github_stargazers.groupby("week")["user"]
        .count()
        .reset_index(name="count")
        .sort_values(by="week")
    )
    if df.empty:
        raise ValueError("Aggregated DataFrame is empty. No valid 'week' data available.")
    # print("Aggregated DataFrame:\n", df.head())  # Debugging line
    return df



@asset
def github_stars_notebook(github_stargazers_by_week: pd.DataFrame) -> str:
    """
    Generates a Jupyter notebook that visualizes GitHub stars by week using Plotly.
    """
    # Ensure we have the required packages
    notebook_setup = '''
    !pip install -q plotly kaleido
    '''
    
    # Convert timestamps to strings before converting to dict
    df_copy = github_stargazers_by_week.copy()
    df_copy['week'] = df_copy['week'].dt.strftime('%Y-%m-%d')
    
    # Convert the DataFrame to a Python literal representation
    df_repr = df_copy.to_dict()
    df_code = f"pd.DataFrame({df_repr})"
    
    notebook = new_notebook()
    # Add setup cell
    notebook.cells.append(new_code_cell(notebook_setup))
    # Add main content cell
    notebook.cells.append(new_code_cell(
        NOTEBOOK_TEMPLATE_START + 
        df_code + 
        NOTEBOOK_TEMPLATE_END
    ))

    # Execute the notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    try:
        ep.preprocess(notebook, resources={})
    except Exception as e:
        print("Notebook Execution Error:", e)
        raise RuntimeError(f"Error executing notebook: {str(e)}") from e

    return nbformat.writes(notebook)



@asset
def github_stars_notebook_gist(context: AssetExecutionContext, github_stars_notebook: str) -> str:
    """
    Uploads the generated Jupyter notebook as a private GitHub Gist.

    Args:
        context (AssetExecutionContext): Dagster asset execution context.
        github_stars_notebook (str): The serialized notebook content.

    Returns:
        str: The URL of the created Gist.
    """
    try:
        github_instance = Github(ACCESS_TOKEN)
        user = github_instance.get_user()
        gist = user.create_gist(
            public=False,
            files={
                "github_stars.ipynb": InputFileContent(github_stars_notebook),
            },
        )
        context.log.info(f"Notebook Gist created at {gist.html_url}")
        return gist.html_url
    except GithubException as e:
        context.log.error(f"GitHub API error: {e.data.get('message', str(e))}")
        raise RuntimeError(f"GitHub API error: {e.data.get('message', str(e))}") from e
    except Exception as e:
        context.log.error(f"Unexpected error: {str(e)}")
        raise RuntimeError(f"Unexpected error: {str(e)}") from e


# Optional: Define a Dagster job to group the assets together
from dagster import Definitions, define_asset_job

# Define a job that includes all the assets
github_stars_job = define_asset_job("github_stars_job", selection=["*"])

# Define the Dagster repository
defs = Definitions(
    assets=[
        github_stargazers,
        github_stargazers_by_week,
        github_stars_notebook,
        github_stars_notebook_gist,
    ],
    jobs=[github_stars_job],
)