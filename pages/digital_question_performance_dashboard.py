import config
import dash
import pandas as pd

from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from sqlalchemy import create_engine


# Register the page with Dash
dash.register_page(__name__)

# Create the connection string for the database
connection_string = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
# Create a SQLAlchemy engine for database interaction
engine = create_engine(connection_string)

###################################  Digital Question Performance Dashboard Logic ###################################


@callback(
    Output("dig-qlp-data-table", "data"),
    Input("dig-qlp-qset-dropdown", "value"),
    prevent_initial_call=True,
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
    question_level_dt_query = f"""
    SELECT
        lpd.question_set_id,
        qs.x_id AS question_set_uid,
        qs.sequence qs_seq,
        lpd.question_id,
        ques.x_id AS question_uid,
        qsqm.sequence q_seq,
        lpd.learner_id,
        lpd.score,
        lpd.updated_at,
        DATE(lpd.updated_at) AS updated_date
    FROM learner_proficiency_question_level_data lpd
    LEFT JOIN question_set_question_mapping qsqm ON qsqm.question_set_id=lpd.question_set_id AND qsqm.question_id=lpd.question_id
    LEFT JOIN question_set qs ON qs.identifier=lpd.question_set_id
    LEFT JOIN question ques ON ques.identifier=lpd.question_id
    WHERE qs.x_id='{selected_qset}'
    """

    # Execute the query and store the result in a DataFrame
    question_level_data = pd.read_sql(question_level_dt_query, engine)

    # Return an empty DataFrame if no data is found
    if question_level_data.empty:
        return pd.DataFrame([]).to_dict("records")

    # Sort data by question set ID, learner ID, and question sequence
    question_level_data.sort_values(
        by=["question_set_id", "learner_id", "q_seq"], inplace=True
    )

    # Calculate time taken by each learner to solve respective questions (in seconds)
    question_level_data["question_time_taken"] = (
        question_level_data.groupby(["question_set_id", "learner_id", "updated_date"])[
            "updated_at"
        ]
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
            ]
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


##### DROPDOWNS OPTIONS
# Fetch distinct question set IDs from the database
question_set_ids = pd.read_sql(
    "SELECT DISTINCT(x_id) AS qset_id FROM question_set",
    engine,
)

###################################  Digital Question Performance Dashboard Layout ###################################

# Define the layout of the Dash app
layout = html.Div(
    [
        html.H1("Question Level Performance Dashboard"),
        html.Div(
            [
                # Dropdown for selecting question set ID
                dcc.Dropdown(
                    id="dig-qlp-qset-dropdown",
                    options=[
                        {"label": qset_id, "value": qset_id}
                        for qset_id in question_set_ids["qset_id"]
                        .sort_values()
                        .unique()
                        if qset_id
                    ],
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
