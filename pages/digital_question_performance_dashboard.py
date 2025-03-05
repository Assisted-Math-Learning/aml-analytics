import dash
import pandas as pd

from dash import Dash, Input, Output, callback, dash_table, dcc, html

from db_utils import (
    get_all_question_sets,
    get_question_level_data,
    get_repository_names_list,
)


# Register the page with Dash
dash.register_page(__name__)


###################################  Digital Question Performance Dashboard Logic ###################################


def get_repo_options():
    # Get the list of repositories from the database
    repo_options = [
        {"label": repo, "value": repo} for repo in get_repository_names_list()
    ]
    return repo_options


@callback(
    Output("dig-qlp-data-table", "data"),
    Input("dig-qlp-qset-dropdown", "value"),
)
def update_table(selected_qset):

    # Return an empty DataFrame if no question set is selected
    if not selected_qset:
        return pd.DataFrame([]).to_dict("records")

    # SQL query to fetch question level data for the selected question set
    # The query fetches detailed question-level data for a specific question set identified by selected_qset,
    # including question set and question identifiers, sequences, learner IDs, scores, and update timestamps.
    # It joins data from learner proficiency, question set mappings, question sets, and questions to provide a comprehensive view of each question's performance.
    # This allows for analysis of individual question performance within the selected question set.

    # Execute the query and store the result in a DataFrame
    question_level_data = get_question_level_data(selected_qset)

    # Return an empty DataFrame if no data is found
    if question_level_data.empty:
        return pd.DataFrame([]).to_dict("records")

    # Sort data by question set ID, learner ID, and question sequence
    question_level_data.sort_values(
        by=["question_set_id", "learner_id", "q_seq"], inplace=True
    )

    # Calculate time taken by each learner to solve respective questions (in seconds)
    question_level_data["question_time_taken"] = (
        question_level_data.groupby(
            ["question_set_id", "learner_id", "updated_date"], observed=True
        )["updated_at"]
        .diff()
        .dt.total_seconds()
    )
    # Fill NaN values with 0 for the first question of each group
    question_level_data["question_time_taken"] = question_level_data[
        "question_time_taken"
    ].fillna(0)

    # Aggregate following metrics of every question:
    # 1. Count of learners attempted that question
    # 2. Median accuracy of all learners in that question
    # 3. Average accuracy in that question
    # 4. Average time taken by learners to complete that question
    final_df = (
        question_level_data.groupby(
            [
                "question_set_id",
                "question_set_uid",
                "qs_seq",
                "question_id",
                "question_uid",
                "q_seq",
            ],
            observed=True,
        )
        .agg(
            learners_count=("learner_id", "nunique"),
            median_accuracy=("score", "median"),
            average_accuracy=("score", "mean"),
            average_time_taken=("question_time_taken", "mean"),
        )
        .reset_index()
        .sort_values(by=["qs_seq", "q_seq"])
    )

    # Format accuracy as percentage
    final_df["median_accuracy"] = final_df["median_accuracy"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )
    final_df["average_accuracy"] = final_df["average_accuracy"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )

    # Round average time taken to 3 decimal places
    final_df["average_time_taken"] = final_df["average_time_taken"].apply(
        lambda x: round(x, 3) if pd.notna(x) else x
    )

    # Return the final DataFrame as a dictionary
    return final_df.to_dict("records")


@callback(
    Output("dig-qlp-qset-dropdown", "options"),
    Input("dig-qlp-repo-dropdown", "value"),
)
def update_qset_options(selected_repo):
    # Fetch all question set IDs for the selected repository
    question_set_ids = get_all_question_sets(selected_repo)

    # Return a list of dictionaries with question set IDs and names
    return [{"label": qset_id, "value": qset_id} for qset_id in question_set_ids]


###################################  Digital Question Performance Dashboard Layout ###################################


# Define the layout of the Dash app
def question_performance_layout():
    return html.Div(
        [
            html.H1("Question Level Performance Dashboard"),
            html.Div(
                [
                    # Dropdown for selecting repository name
                    dcc.Dropdown(
                        id="dig-qlp-repo-dropdown",
                        placeholder="Select Repository",
                        options=get_repo_options(),
                        style={"width": "300px", "margin": "10px"},
                    ),
                    # Dropdown for selecting question set ID
                    dcc.Dropdown(
                        id="dig-qlp-qset-dropdown",
                        placeholder="Select Question Set ID",
                        style={"width": "300px", "margin": "10px"},
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "flex-start",  # Center items horizontally
                    "alignItems": "center",  # Align items vertically
                    "flexWrap": "wrap",  # Allow items to wrap to the next line if needed
                    "marginBottom": "10px",
                },
            ),
            # Loading spinner for the data table
            dcc.Loading(
                id="dig-qlp-loading-table",
                type="circle",
                children=[
                    # Data table to display question level performance data
                    dash_table.DataTable(
                        id="dig-qlp-data-table",
                        columns=[
                            {"name": "Question Set UID", "id": "question_set_uid"},
                            {"name": "Q-Set Sequence", "id": "qs_seq"},
                            {"name": "Question UID", "id": "question_uid"},
                            {"name": "Question Sequence", "id": "q_seq"},
                            {"name": "Learners Count", "id": "learners_count"},
                            {"name": "Median Accuracy", "id": "median_accuracy"},
                            {"name": "Average Accuracy", "id": "average_accuracy"},
                            {
                                "name": "Average Time Taken (In Sec)",
                                "id": "average_time_taken",
                            },
                        ],
                        style_table={
                            "overflow": "auto",
                            "maxWidth": "100%",
                            "marginTop": "20px",
                            "marginBottom": "50px",
                            "maxHeight": "800px",
                        },
                        style_data={
                            "whiteSpace": "pre-wrap",  # Preserve newlines
                            "height": "auto",  # Adjust row height
                            "textAlign": "center",
                            "border": "1px solid black",
                        },
                        style_header={
                            "fontWeight": "bold",
                            "color": "black",
                            "border": "1px solid #004494",
                            "backgroundColor": "#A3C1E0",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "position": "sticky",
                            "top": "0",
                            "text-align": "center",
                        },  # Sticky header
                        style_cell={
                            "textAlign": "center",
                            "minWidth": "150px",
                            "maxWidth": "300px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        data=[],
                    )
                ],
            ),
        ]
    )


layout = question_performance_layout
