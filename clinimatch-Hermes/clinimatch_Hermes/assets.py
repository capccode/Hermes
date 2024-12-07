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

# Load environment variables from a .env file if present
load_dotenv()

# Securely handle the GitHub access token
ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("GITHUB_ACCESS_TOKEN environment variable not set")


# Define the notebook template with placeholders for serialized data
NOTEBOOK_TEMPLATE = """
import json
import pandas as pd
import plotly.express as px
from io import StringIO

# Load the data
serialized_data = {serialized_data}
# Wrap the serialized_data string in StringIO
github_stargazers_by_week = pd.read_json(StringIO(serialized_data), orient="split")

print("Loaded DataFrame Columns:", github_stargazers_by_week.columns.tolist())
print("Loaded DataFrame Head:\\n", github_stargazers_by_week.head())

# Convert 'week' from milliseconds to datetime
github_stargazers_by_week['week'] = pd.to_datetime(github_stargazers_by_week['week'], unit='ms')

# Plot the data using Plotly
df_last_year = github_stargazers_by_week.tail(52)
fig = px.bar(
    df_last_year,
    x='week',
    y='count',
    title='GitHub Stars by Week (Last 52 Weeks)',
    labels={'week': 'Week', 'count': 'Star Count'},
    template='plotly_white'
)

# Enhance the plot
fig.update_layout(
    xaxis_title='Week',
    yaxis_title='Star Count',
    xaxis_tickangle=-45
)

fig.update_xaxes(
    tickformat="%b %d, %Y",  # Formats dates as "Jan 01, 2023"
    tickangle=-45
)

fig.show()
"""


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
    print("Aggregated DataFrame:\n", df.head())  # Debugging line
    return df



@asset
def github_stars_notebook(github_stargazers_by_week: pd.DataFrame) -> str:
    """
    Generates a Jupyter notebook that visualizes GitHub stars by week using Plotly.

    Args:
        github_stargazers_by_week (pd.DataFrame): Aggregated stargazers data by week.

    Returns:
        str: The serialized notebook in JSON format.
    """
    # Serialize the DataFrame to JSON
    serialized_data = github_stargazers_by_week.to_json(orient="split")
    print("Serialized Data:", serialized_data)  # Debugging line

    # Create a notebook object using the template
    notebook_content = NOTEBOOK_TEMPLATE.format(serialized_data=json.dumps(serialized_data))
    notebook = new_notebook()
    notebook.cells.append(new_code_cell(notebook_content))

    # Execute the notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    try:
        ep.preprocess(notebook, resources={})
    except Exception as e:
        print("Notebook Execution Error:", e)  # Debugging line
        raise RuntimeError(f"Error executing notebook: {str(e)}") from e

    # Serialize the notebook into JSON format
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
