import dash
import pandas as pd
import os
import psutil

from dash import Dash, Input, Output, callback, dash_table, dcc, html

from db_utils import (
    get_all_learners_data_df,
    get_all_question_sets,
    get_grades_list,
    get_l2_skills_list,
    get_l3_skills_list,
    get_qset_types_list,
    get_repository_names_list,
)


# Register the page
dash.register_page(__name__)

""" UTILS """
# Define color coding for operations and grades
operation_color_coding = {
    "Addition": "#6dbdd1",
    "Subtraction": "#8cddfa",
    "Multiplication": "#ebf3fc",
    "Division": "#9baef2",
}

grade_color_coding = {
    "class-one": "#bfdee3",
    "class-two": "#d9f3fa",
    "class-three": "#e6f6fa",
    "class-four": "#d8e3e6",
    "class-five": "#f0f4f5",
}

# Define operation priorities for sorting
operations_priority = {
    "Addition": 0,
    "Subtraction": 1,
    "Multiplication": 2,
    "Division": 3,
}

# Define operation types
operations = ["Addition", "Subtraction", "Multiplication", "Division"]


###################################  Digital QSet Performance Dashboard Logic ###################################


def get_question_set_data(
    selected_repo,
    selected_qsets,
    selected_operation,
    selected_l2_skill,
    selected_l3_skill,
    selected_sheet_type,
):
    all_learners_data = get_all_learners_data_df()[
        [
            "repo_name",
            "question_set_id",
            "qset_uid",
            "learner_id",
            "operation",
            "qset_grade",
            "sequence",
            "purpose",
            "qset_name",
            "l1_skill",
            "l2_skill",
            "l3_skill",
            "updated_at",
            "score",
            "status",
        ]
    ]
    completed_question_sets_data = all_learners_data[
        all_learners_data["status"] == "completed"
    ]

    # Add conditions based on selected filters
    if selected_repo:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["repo_name"] == selected_repo
        ]

    if selected_qsets:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["qset_uid"].isin(selected_qsets)
        ]

    if selected_operation:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["l1_skill"] == selected_operation
        ]

    if selected_l2_skill:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["l2_skill"] == selected_l2_skill
        ]

    if selected_l3_skill:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["l3_skill"] == selected_l3_skill
        ]

    if selected_sheet_type:
        completed_question_sets_data = completed_question_sets_data[
            completed_question_sets_data["purpose"] == selected_sheet_type
        ]

    return completed_question_sets_data


def get_repo_options():
    print("Digital QSet Performance Dashboard: repo have been called")
    # Get the list of repositories from the database
    repo_options = [
        {"label": repo, "value": repo} for repo in get_repository_names_list()
    ]
    return repo_options


def get_qset_type_options():
    print("Digital QSet Performance Dashboard: qset type have been called")
    # Get the list of qset types from the database
    qset_type_options = [
        {"label": qset_type, "value": qset_type} for qset_type in get_qset_types_list()
    ]
    return qset_type_options


def get_l2_skill_options():
    print("Digital QSet Performance Dashboard: l2 skill have been called")
    # Get the list of l2 skills from the database
    l2_skill_options = [
        {"label": l2_skill, "value": l2_skill}
        for l2_skill in get_l2_skills_list()
        if l2_skill
    ]
    return l2_skill_options


def get_l3_skill_options():
    print("Digital QSet Performance Dashboard: l3 skill have been called")
    # Get the list of l3 skills from the database
    l3_skill_options = [
        {"label": l3_skill, "value": l3_skill}
        for l3_skill in get_l3_skills_list()
        if l3_skill
    ]
    return l3_skill_options


@callback(
    Output("dig-qsp-data-table", "data"),
    Input("dig-qsp-repo-dropdown", "value"),
    Input("dig-qsp-qset-dropdown", "value"),
    Input("dig-qsp-operations-dropdown", "value"),
    Input("dig-qsp-l2-skill-dropdown", "value"),
    Input("dig-qsp-l3-skill-dropdown", "value"),
    Input("dig-qsp-qset-types-dropdown", "value"),
)
def update_table(
    selected_repo,
    selected_qsets,
    selected_operation,
    selected_l2_skill,
    selected_l3_skill,
    selected_sheet_type,
):
    # Return empty data if no filters are selected
    if (
        (not selected_repo)
        and (not selected_qsets)
        and (not selected_l3_skill)
        and (not selected_l2_skill)
        and (not selected_operation)
        and (not selected_sheet_type)
    ):
        return pd.DataFrame([]).to_dict("records")

    # The query retrieves detailed question set data for completed learner journeys, filtering by selected question sets, operations, skills, and sheet type.
    # It constructs conditions dynamically based on user selections and executes the query to fetch the filtered data.
    question_set_data = get_question_set_data(
        selected_repo,
        selected_qsets,
        selected_operation,
        selected_l2_skill,
        selected_l3_skill,
        selected_sheet_type,
    )

    # Return empty data if no results are found
    if question_set_data.empty:
        return pd.DataFrame([]).to_dict("records")

    question_set_data["updated_date"] = question_set_data["updated_at"].dt.date

    # Group data by learner and calculate time spent on and marks scored in respective qsets on each date
    question_set_data_per_learner = (
        question_set_data.groupby(
            [
                "question_set_id",
                "qset_uid",
                "learner_id",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
                "updated_date",
            ],
            observed=True,
        )
        .agg(
            min_timestamp=("updated_at", "min"),
            max_timestamp=("updated_at", "max"),
            score=("score", "sum"),
            count=("score", "count"),
        )
        .reset_index()
    )

    # Calculate time difference in seconds
    question_set_data_per_learner["time_diff"] = (
        question_set_data_per_learner["max_timestamp"]
        - question_set_data_per_learner["min_timestamp"]
    ).dt.total_seconds()

    # Aggregate total time taken, total marks scored and total questions attempted of every qset attempted by learners
    final_question_set_data_per_learner = (
        question_set_data_per_learner.groupby(
            [
                "question_set_id",
                "qset_uid",
                "learner_id",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
            ],
            observed=True,
        )
        .agg(
            total_time=("time_diff", "sum"),
            total_score=("score", "sum"),
            total_count=("count", "sum"),
        )
        .reset_index()
    )

    # Calculate accuracy of every qset attempted by each learner
    final_question_set_data_per_learner["accuracy"] = (
        final_question_set_data_per_learner["total_score"]
        / final_question_set_data_per_learner["total_count"]
    )

    # Aggregate following metrics of every qset:
    # 1. Count of learners attempted that qset
    # 2. Median accuracy of all learners in that qset
    # 3. Average accuracy in that qset
    # 4. Median time taken by learners to complete that qset
    # 5. Average time taken by learners to complete that qset
    final_df = (
        final_question_set_data_per_learner.groupby(
            [
                "question_set_id",
                "qset_uid",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
            ],
            observed=True,
        )
        .agg(
            count_of_learners=("learner_id", "nunique"),
            median_accuracy=("accuracy", "median"),
            average_accuracy=("accuracy", "mean"),
            median_time=("total_time", "median"),
            mean_time=("total_time", "mean"),
        )
        .reset_index()
    )

    # Format accuracy and time for display
    final_df["median_accuracy"] = final_df["median_accuracy"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )
    final_df["average_accuracy"] = final_df["average_accuracy"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )

    final_df["median_time"] = final_df["median_time"].apply(
        lambda x: round(x / 60, 2) if pd.notna(x) else x
    )
    final_df["mean_time"] = final_df["mean_time"].apply(
        lambda x: round(x / 60, 2) if pd.notna(x) else x
    )

    # Fetch grades
    grades = get_grades_list()
    # Map grades to their priorities for sorting
    grades_priority = grades.set_index("grade").to_dict().get("id")

    # Map operation and grade to their order for sorting
    final_df.loc[:, "operation_order"] = final_df["operation"].map(operations_priority)
    final_df.loc[:, "qset_grade_order"] = final_df["qset_grade"].map(grades_priority)

    # Sort the dataframe by operation, grade, and sequence
    final_df.sort_values(
        by=["operation_order", "qset_grade_order", "sequence"],
        ascending=[True, True, True],
        inplace=True,
    )

    process = psutil.Process(os.getpid())
    print(
        f"Digital QSet Performance Dashboard Memory Usage: {process.memory_info().rss / (1024 * 1024)} MB"
    )

    return final_df.to_dict("records")


##### DROPDOWNS OPTIONS


@callback(
    Output("dig-qsp-qset-dropdown", "options"),
    Input("dig-qsp-repo-dropdown", "value"),
)
def update_qset_options(selected_repo):
    # Fetch all question set IDs for the selected repository
    question_set_ids = get_all_question_sets(selected_repo)

    # Return a list of dictionaries with question set IDs and names
    return [{"label": qset_id, "value": qset_id} for qset_id in question_set_ids]


###################################  Digital QSet Performance Dashboard Layout ###################################


# Define the layout of the Dash app
def qset_performance_layout():
    return html.Div(
        [
            html.H1("Question-Set Level Performance Dashboard"),
            html.Div(
                [
                    # Dropdown for selecting repository name
                    dcc.Dropdown(
                        id="dig-qsp-repo-dropdown",
                        placeholder="Select Repository",
                        options=get_repo_options(),
                        style={"width": "300px", "margin": "10px"},
                    ),
                    # Dropdown for selecting question set IDs
                    dcc.Dropdown(
                        id="dig-qsp-qset-dropdown",
                        multi=True,
                        placeholder="Select Question Set ID",
                        style={"width": "300px", "margin": "10px"},
                    ),
                    # Dropdown for selecting operations
                    dcc.Dropdown(
                        id="dig-qsp-operations-dropdown",
                        options=[
                            {"label": operation, "value": operation}
                            for operation in [
                                "Addition",
                                "Subtraction",
                                "Multiplication",
                                "Division",
                            ]
                        ],
                        placeholder="Select Operation",
                        style={"width": "300px", "margin": "10px"},
                    ),
                    # Dropdown for selecting L2 skills
                    dcc.Dropdown(
                        id="dig-qsp-l2-skill-dropdown",
                        placeholder="Select L2 Skill",
                        options=get_l2_skill_options(),
                        style={"width": "300px", "margin": "10px"},
                    ),
                    # Dropdown for selecting L3 skills
                    dcc.Dropdown(
                        id="dig-qsp-l3-skill-dropdown",
                        placeholder="Select L3 Skill",
                        options=get_l3_skill_options(),
                        style={"width": "350px", "margin": "10px"},
                        optionHeight=55,
                    ),
                    # Dropdown for selecting question set types
                    dcc.Dropdown(
                        id="dig-qsp-qset-types-dropdown",
                        placeholder="Select Question Set Type",
                        options=get_qset_type_options(),
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
                id="dig-qsp-loading-table",
                type="circle",
                children=[
                    dash_table.DataTable(
                        id="dig-qsp-data-table",
                        columns=[
                            {"name": "Operation", "id": "operation"},
                            {"name": "Class", "id": "qset_grade"},
                            {"name": "Question Set UID", "id": "qset_uid"},
                            {"name": "Question Set Sequence", "id": "sequence"},
                            {"name": "Question Set Type", "id": "purpose"},
                            {"name": "Question Set Name", "id": "qset_name"},
                            {"name": "L2", "id": "l2_skill"},
                            {"name": "L3", "id": "l3_skill"},
                            {"name": "Learners Count", "id": "count_of_learners"},
                            {"name": "Median Accuracy", "id": "median_accuracy"},
                            {"name": "Average Accuracy", "id": "average_accuracy"},
                            {"name": "Median Time Taken (in min)", "id": "median_time"},
                            {"name": "Average Time Taken (in min)", "id": "mean_time"},
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
                            "minWidth": "100px",
                            "maxWidth": "150px",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                        data=[],
                        style_data_conditional=[
                            *(
                                {
                                    "if": {
                                        "filter_query": f"{{operation}} = '{operation}'",
                                        "column_id": f"operation",
                                    },
                                    "backgroundColor": operation_color_coding[
                                        operation
                                    ],
                                }
                                for operation in operation_color_coding.keys()
                            ),
                            *(
                                {
                                    "if": {
                                        "filter_query": f"{{qset_grade}} = '{grade}'",
                                        "column_id": f"qset_grade",
                                    },
                                    "backgroundColor": grade_color_coding[grade],
                                }
                                for grade in grade_color_coding.keys()
                            ),
                        ],
                    )
                ],
            ),
        ]
    )


layout = qset_performance_layout
