import config
import dash
import pandas as pd

from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from sqlalchemy import create_engine


# Register the page
dash.register_page(__name__)

# Create the connection string for the database
connection_string = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
engine = create_engine(connection_string)

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
    selected_qsets,
    selected_operation,
    selected_l2_skill,
    selected_l3_skill,
    selected_sheet_type,
):
    # Base query to fetch question set data
    question_set_dt_query = f"""
    SELECT qs.identifier AS question_set_id,
        qs.x_id AS question_set_uid,
        lpd.learner_id AS learner_id,
        qs.taxonomy->'l1_skill'->'name'->>'en' AS operation,
        qs.taxonomy->'class'->'name'->>'en' AS qset_grade,
        qs.sequence AS sequence,
        qs.purpose AS purpose,
        qs.title->>'en' AS qset_name,
        qs.taxonomy->'l2_skill'->0->'name'->>'en' AS l2_skill,
        qs.taxonomy->'l3_skill'->0->'name'->>'en' AS l3_skill,
        lpd.score AS score,
        lpd.updated_at,
        DATE(lpd.updated_at) AS updated_date
    FROM learner_journey lj
    LEFT JOIN learner_proficiency_question_level_data lpd ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN question_set qs ON qs.identifier = lj.question_set_id
    """

    # List to hold query conditions
    conditions = []

    # Add condition for completed status
    status_query = "lj.status='completed'"
    conditions.append(status_query)

    # Add conditions based on selected filters
    if selected_qsets:
        qsets_query = "qs.x_id IN ({})".format(
            ", ".join(f"'{qset_name}'" for qset_name in selected_qsets)
        )
        conditions.append(qsets_query)

    if selected_operation:
        conditions.append(
            "qs.taxonomy->'l1_skill'->'name'->>'en' = '{}'".format(selected_operation)
        )

    if selected_l2_skill:
        conditions.append(
            "qs.taxonomy->'l2_skill'->0->'name'->>'en' = '{}'".format(selected_l2_skill)
        )

    if selected_l3_skill:
        conditions.append(
            "qs.taxonomy->'l3_skill'->0->'name'->>'en' = '{}'".format(selected_l3_skill)
        )

    if selected_sheet_type:
        conditions.append("qs.purpose = '{}'".format(selected_sheet_type))

    # Append conditions to the query
    question_set_dt_query += " WHERE " + " AND ".join(conditions)

    # Execute the query and fetch data
    question_set_data = pd.read_sql(question_set_dt_query, engine)
    return question_set_data


@callback(
    Output("dig-qsp-data-table", "data"),
    Input("dig-qsp-qset-dropdown", "value"),
    Input("dig-qsp-operations-dropdown", "value"),
    Input("dig-qsp-l2-skill-dropdown", "value"),
    Input("dig-qsp-l3-skill-dropdown", "value"),
    Input("dig-qsp-qset-types-dropdown", "value"),
    prevent_initial_call=True,
)
def update_table(
    selected_qsets,
    selected_operation,
    selected_l2_skill,
    selected_l3_skill,
    selected_sheet_type,
):
    # Return empty data if no filters are selected
    if (
        (not selected_qsets)
        and (not selected_l3_skill)
        and (not selected_l2_skill)
        and (not selected_operation)
        and (not selected_sheet_type)
    ):
        return pd.DataFrame([]).to_dict("records")

    # The query retrieves detailed question set data for completed learner journeys, filtering by selected question sets, operations, skills, and sheet type.
    # It constructs conditions dynamically based on user selections and executes the query to fetch the filtered data.
    question_set_data = get_question_set_data(
        selected_qsets,
        selected_operation,
        selected_l2_skill,
        selected_l3_skill,
        selected_sheet_type,
    )

    # Return empty data if no results are found
    if question_set_data.empty:
        return pd.DataFrame([]).to_dict("records")

    # Group data by learner and calculate time spent on and marks scored in respective qsets on each date
    question_set_data_per_learner = (
        question_set_data.groupby(
            [
                "question_set_id",
                "question_set_uid",
                "learner_id",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
                "updated_date",
            ]
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
                "question_set_uid",
                "learner_id",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
            ]
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
                "question_set_uid",
                "operation",
                "qset_grade",
                "sequence",
                "purpose",
                "qset_name",
                "l2_skill",
                "l3_skill",
            ]
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

    # Map operation and grade to their order for sorting
    final_df.loc[:, "operation_order"] = final_df["operation"].map(operations_priority)
    final_df.loc[:, "qset_grade_order"] = final_df["qset_grade"].map(grades_priority)

    # Sort the dataframe by operation, grade, and sequence
    final_df.sort_values(
        by=["operation_order", "qset_grade_order", "sequence"],
        ascending=[True, True, True],
        inplace=True,
    )

    return final_df.to_dict("records")


##### DROPDOWNS OPTIONS
# Fetch distinct Qset types from the database
qset_types = pd.read_sql("SELECT DISTINCT(purpose) FROM question_set", engine)

# Fetch distinct question set identifiers
question_set_ids = pd.read_sql(
    "SELECT DISTINCT(x_id) AS qset_id FROM question_set", engine
)

# Fetch distinct L2 skill types
l2_skills = pd.read_sql(
    "SELECT DISTINCT(taxonomy->'l2_skill'->0->'name'->>'en') AS l2_skill FROM question_set",
    engine,
)

# Fetch distinct L3 skill types
l3_skills = pd.read_sql(
    "SELECT DISTINCT(taxonomy->'l3_skill'->0->'name'->>'en') AS l3_skill FROM question_set",
    engine,
)

# Fetch grades from the database
grades = pd.read_sql("SELECT id, cm.name->>'en' AS grade FROM class_master cm", engine)

# Map grades to their priorities for sorting
grades_priority = grades.set_index("grade").to_dict().get("id")

###################################  Digital QSet Performance Dashboard Layout ###################################

# Define the layout of the Dash app
layout = html.Div(
    [
        html.H1("Question-Set Level Performance Dashboard"),
        html.Div(
            [
                # Dropdown for selecting question set IDs
                dcc.Dropdown(
                    id="dig-qsp-qset-dropdown",
                    options=[
                        {"label": qset_id, "value": qset_id}
                        for qset_id in question_set_ids["qset_id"]
                        .sort_values()
                        .unique()
                        if qset_id
                    ],
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
                    options=[
                        {"label": l2_skill, "value": l2_skill}
                        for l2_skill in l2_skills["l2_skill"].sort_values().unique()
                        if l2_skill
                    ],
                    placeholder="Select L2 Skill",
                    style={"width": "300px", "margin": "10px"},
                ),
                # Dropdown for selecting L3 skills
                dcc.Dropdown(
                    id="dig-qsp-l3-skill-dropdown",
                    options=[
                        {"label": l3_skill, "value": l3_skill}
                        for l3_skill in l3_skills["l3_skill"].sort_values().unique()
                        if l3_skill
                    ],
                    placeholder="Select L3 Skill",
                    style={"width": "350px", "margin": "10px"},
                    optionHeight=55,
                ),
                # Dropdown for selecting question set types
                dcc.Dropdown(
                    id="dig-qsp-qset-types-dropdown",
                    options=[
                        {"label": qset_type, "value": qset_type}
                        for qset_type in qset_types["purpose"].sort_values().unique()
                    ],
                    placeholder="Select Question Set Type",
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
                        {"name": "Question Set UID", "id": "question_set_uid"},
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
                                "backgroundColor": operation_color_coding[operation],
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
