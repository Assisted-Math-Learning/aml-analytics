import dash
import pandas as pd
from dash import Dash, dash_table, dcc, html, Input, Output, callback

from db_utils import (
    LAST_QUESTION_PER_QSET_GRADE_KEY,
    get_data,
    get_grades_list,
    get_non_diagnostic_data,
    get_schools_list,
)

# Register the page
# app = Dash(__name__)
# server = app.server
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

# Define operation priorities
operations_priority = {
    "Addition": 0,
    "Subtraction": 1,
    "Multiplication": 2,
    "Division": 3,
}


# Function to set the current grade for each learner
# The current_grade is determined by evaluating the learner's progress through sorted question sets,
# checking if they have reached their target grade or need to progress to the next grade.
# If the learner's target_grade matches the qset_grade, current_grade is set to "target-achieved".
# Otherwise, it progresses to the next grade in sequence if conditions are met, or remains the same.
def set_curr_grade(group):
    # Sort the group by operation and grade order
    group.sort_values(
        by=["operation_order", "qset_grade_order"], ascending=[True, True], inplace=True
    )
    # Get the last record of the group
    last_record = group.iloc[-1]
    target_grade = last_record["target_grade"]
    qset_grade = last_record["qset_grade"]
    next_grade = last_record["next_grade"]
    curr_grade = qset_grade

    # Check if this is the last record for the learner or has next_grade
    if last_record["is_last"] == True or not pd.isna(next_grade):
        # Check if the target_grade and qset_grade match
        if target_grade == qset_grade:
            curr_grade = "target-achieved"
        elif qset_grade == "class-one":
            curr_grade = "class-two"
        elif qset_grade == "class-two":
            curr_grade = "class-three"
        elif qset_grade == "class-three":
            curr_grade = "class-four"
        elif qset_grade == "class-four":
            curr_grade = "class-five"
        elif qset_grade == "class-five":
            curr_grade = "class-six"

    group["current_grade"] = curr_grade
    return group


# Map current grades to target grades
target_grade_map = {
    "class-two": "class-one",
    "class-three": "class-two",
    "class-four": "class-three",
    "class-five": "class-four",
    "class-six": "class-five",
}

###################################  Digital Learner Progress Dashboard Logic ###################################


@callback(
    Output("dig-l-prog-data-table", "data"),
    Output("dig-l-prog-schools-dropdown", "options"),
    Input("dig-l-prog-schools-dropdown", "value"),
)
def update_table(selected_school):
    non_diagnostic_data = get_non_diagnostic_data()
    print("Non diagnostic data", non_diagnostic_data.head(1))
    last_question_per_qset_grade = get_data(LAST_QUESTION_PER_QSET_GRADE_KEY)
    print("Last question per qset grade", last_question_per_qset_grade.head(1))
    # Map question set last question to learners attempt data
    # It is done to know whether the learner has attempted the last question of that qset or not.
    # Based, on that the learner we'll be able to know that whether the learner has solved the last qset of that qset grade or not.
    learner_progress_data = pd.merge(
        non_diagnostic_data,
        last_question_per_qset_grade,
        on=["operation", "qset_grade", "question_set_id", "question_id"],
        how="left",
        indicator=True,
    )
    # Determine if the last question is found
    learner_progress_data["is_found"] = learner_progress_data["_merge"].apply(
        lambda x: 1 if x == "both" else 0
    )
    learner_progress_data.drop(columns=["_merge"], inplace=True)

    # Filter the DataFrame based on the selected schools
    if selected_school:
        learner_progress_df = learner_progress_data[
            learner_progress_data["school"] == selected_school
        ].copy()
    else:
        learner_progress_df = learner_progress_data.copy()

    # Group by learner, grade, operation, qset_grade and calculate if the last question is answered for the qset_grade of that operation by the learner
    learner_progress_df = (
        learner_progress_df.groupby(
            ["learner_id", "grade", "operation", "qset_grade"], observed=False
        )
        .agg(is_last=("is_found", "any"))
        .reset_index()
    )

    # Map operation and grade to their order
    learner_progress_df.loc[:, "operation_order"] = learner_progress_df[
        "operation"
    ].map(operations_priority)

    # Fetch grades
    grades = get_grades_list()
    # Map grades to their priorities for sorting
    grades_priority = grades.set_index("grade").to_dict().get("id")

    learner_progress_df.loc[:, "qset_grade_order"] = learner_progress_df[
        "qset_grade"
    ].map(grades_priority)

    # Sort the dataframe by learner, operation, and qset_grade order before mapping the next qset_grade on each record
    learner_progress_df.sort_values(
        by=["learner_id", "operation_order", "qset_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    # Determine the next grade for each learner
    learner_progress_df["next_grade"] = learner_progress_df.groupby("learner_id")[
        "qset_grade"
    ].shift(-1)

    # Map current grades to target grades
    target_grade_map = {
        "class-two": "class-one",
        "class-three": "class-two",
        "class-four": "class-three",
        "class-five": "class-four",
        "class-six": "class-five",
    }

    learner_progress_df["target_grade"] = learner_progress_df["grade"].map(
        target_grade_map
    )

    # Apply the function to set the current grade for each learner
    learner_progress_df = (
        learner_progress_df.groupby(
            ["learner_id", "operation", "grade", "target_grade"], observed=False
        )
        .apply(set_curr_grade)
        .reset_index(drop=True)
    )

    # Sort the dataframe again after setting current grades
    learner_progress_df.sort_values(
        by=["learner_id", "operation_order", "qset_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    # Determine the starting grade for each learner
    # Starting grade is the first qset-grade in that operation with which the learner began their journey.
    learner_progress_df["starting_grade"] = learner_progress_df.groupby(
        ["learner_id", "grade", "operation"], observed=False
    )["qset_grade"].transform("first")

    # Count the number of learners for each operation and starting grade
    learner_progress_df["learners_count"] = learner_progress_df.groupby(
        ["operation", "starting_grade", "grade"], observed=False
    )["learner_id"].transform("nunique")

    # Create a pivot table to summarize the progress
    progress_df = pd.pivot_table(
        learner_progress_df,
        index=["operation", "starting_grade", "target_grade"],
        columns=["current_grade"],
        values=["learner_id", "learners_count"],
        aggfunc={"learner_id": "nunique", "learners_count": "first"},
    )
    progress_df = (
        round(progress_df["learner_id"] / progress_df["learners_count"], 2) * 100
    )
    final_df = progress_df.reset_index()

    # Add % sign to respective qset-grades columns
    for column in final_df.columns:
        if column in [
            "class-one",
            "class-two",
            "class-three",
            "class-four",
            "class-five",
            "target-achieved",
        ]:
            final_df[column] = final_df[column].apply(
                lambda x: f"{round(x, 2)} %" if pd.notna(x) else x
            )

    # Calculate the total number of learners for each operation and starting grade
    final_df["learners_count"] = (
        learner_progress_df.groupby(
            ["operation", "starting_grade", "target_grade"], observed=False
        )["learner_id"]
        .agg("nunique")
        .reset_index()["learner_id"]
    )
    final_df["total_count"] = final_df.groupby(["operation", "starting_grade"])[
        "learners_count"
    ].transform("sum")

    # Map operation and grade to their order for sorting
    final_df.loc[:, "operation_order"] = final_df["operation"].map(operations_priority)
    final_df.loc[:, "starting_grade_order"] = final_df["starting_grade"].map(
        grades_priority
    )
    final_df.loc[:, "target_grade_order"] = final_df["target_grade"].map(
        grades_priority
    )

    # Sort the final dataframe
    final_df.sort_values(
        by=["operation_order", "starting_grade_order", "target_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    # Schools Dropdown Options
    school_options = [
        {"label": school, "value": school} for school in get_schools_list()
    ]

    return final_df.to_dict("records"), school_options


""" FINAL LAYOUT OF THE PAGE """

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

# Define the layout of the Dash app
layout = html.Div(
    [
        html.H1("Learners Progress Dashboard"),
        # Dropdown for selecting school
        dcc.Dropdown(
            id="dig-l-prog-schools-dropdown",
            placeholder="Select School",
            style={"width": "300px", "margin": "10px"},
        ),
        # DataTable to display the filtered data
        dcc.Loading(
            id="dig-l-prog-loading-table",
            type="circle",
            children=[
                dash_table.DataTable(
                    id="dig-l-prog-data-table",
                    columns=[
                        {"name": "Operation", "id": "operation"},
                        {"name": "Starting Grade", "id": "starting_grade"},
                        {"name": "Total Count", "id": "total_count"},
                        {"name": "Target Grade", "id": "target_grade"},
                        {"name": "Learners Count", "id": "learners_count"},
                        {"name": "Class One", "id": "class-one"},
                        {"name": "Class Two", "id": "class-two"},
                        {"name": "Class Three", "id": "class-three"},
                        {"name": "Class Four", "id": "class-four"},
                        {"name": "Class Five", "id": "class-five"},
                        {"name": "Target Achieved", "id": "target-achieved"},
                    ],
                    style_table={
                        "overflowX": "auto",
                        "minWidth": "1000px",
                        "maxWidth": "100%",
                        "marginTop": "20px",
                        "marginBottom": "50px",
                        "maxHeight": "600px",
                    },
                    fixed_columns={"headers": True, "data": 4},
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
                                    "filter_query": f"{{starting_grade}} = '{grade}'",
                                    "column_id": f"starting_grade",
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
