import dash
import pandas as pd
from dash import Dash, dash_table, dcc, html, Input, Output, callback
from db_utils import (
    get_last_question_per_qset_grade_df,
    get_grades_list,
    get_non_diagnostic_data,
    get_schools_list,
)

dash.register_page(__name__)

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


def set_curr_grade(group):
    group.sort_values(
        by=["operation_order", "qset_grade_order"], ascending=[True, True], inplace=True
    )
    last_record = group.iloc[-1]
    target_grade = last_record["target_grade"]
    qset_grade = last_record["qset_grade"]
    next_grade = last_record["next_grade"]
    curr_grade = qset_grade

    if last_record["is_last"] or not pd.isna(next_grade):
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


target_grade_map = {
    "class-two": "class-one",
    "class-three": "class-two",
    "class-four": "class-three",
    "class-five": "class-four",
    "class-six": "class-five",
}


def get_school_options():
    # Get the list of schools from the database
    school_options = [
        {"label": school, "value": school} for school in get_schools_list()
    ]
    return school_options


@callback(
    Output("dig-l-prog-data-table", "data"),
    Input("dig-l-prog-schools-dropdown", "value"),
)
def update_table(selected_school):
    non_diagnostic_data = get_non_diagnostic_data()[
        [
            "operation",
            "qset_grade",
            "question_set_id",
            "question_id",
            "school",
            "learner_id",
            "grade",
        ]
    ]
    last_question_per_qset_grade = get_last_question_per_qset_grade_df()

    learner_progress_data = pd.merge(
        non_diagnostic_data,
        last_question_per_qset_grade,
        on=["operation", "qset_grade", "question_set_id", "question_id"],
        how="left",
        indicator=True,
    )
    learner_progress_data["is_found"] = learner_progress_data["_merge"] == "both"
    learner_progress_data.drop(columns=["_merge"], inplace=True)

    if selected_school:
        learner_progress_df = learner_progress_data[
            learner_progress_data["school"] == selected_school
        ].copy()
    else:
        learner_progress_df = learner_progress_data.copy()

    learner_progress_df = (
        learner_progress_df.groupby(
            ["learner_id", "grade", "operation", "qset_grade"], observed=True
        )
        .agg(is_last=("is_found", "any"))
        .reset_index()
    )

    learner_progress_df["operation_order"] = learner_progress_df["operation"].map(
        operations_priority
    )

    grades = get_grades_list()
    grades_priority = grades.set_index("grade").to_dict().get("id")

    learner_progress_df["qset_grade_order"] = learner_progress_df["qset_grade"].map(
        grades_priority
    )

    learner_progress_df.sort_values(
        by=["learner_id", "operation_order", "qset_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    learner_progress_df["next_grade"] = learner_progress_df.groupby(
        "learner_id", observed=True
    )["qset_grade"].shift(-1)

    learner_progress_df["target_grade"] = learner_progress_df["grade"].map(
        target_grade_map
    )

    learner_progress_df = (
        learner_progress_df.groupby(
            ["learner_id", "operation", "grade", "target_grade"], observed=True
        )
        .apply(set_curr_grade)
        .reset_index(drop=True)
    )

    learner_progress_df.sort_values(
        by=["learner_id", "operation_order", "qset_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    learner_progress_df["starting_grade"] = learner_progress_df.groupby(
        ["learner_id", "grade", "operation"], observed=True
    )["qset_grade"].transform("first")

    learner_progress_df["learners_count"] = learner_progress_df.groupby(
        ["operation", "starting_grade", "grade"], observed=True
    )["learner_id"].transform("nunique")

    progress_df = pd.pivot_table(
        learner_progress_df,
        index=["operation", "starting_grade", "target_grade"],
        columns=["current_grade"],
        values=["learner_id", "learners_count"],
        aggfunc={"learner_id": "nunique", "learners_count": "first"},
        observed=True,
    )
    progress_df = (
        round(progress_df["learner_id"] / progress_df["learners_count"], 2) * 100
    )
    final_df = progress_df.reset_index()

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

    final_df["learners_count"] = (
        learner_progress_df.groupby(
            ["operation", "starting_grade", "target_grade"], observed=True
        )["learner_id"]
        .agg("nunique")
        .reset_index()["learner_id"]
    )
    final_df["total_count"] = final_df.groupby(
        ["operation", "starting_grade"], observed=True
    )["learners_count"].transform("sum")

    final_df["operation_order"] = final_df["operation"].map(operations_priority)
    final_df["starting_grade_order"] = final_df["starting_grade"].map(grades_priority)
    final_df["target_grade_order"] = final_df["target_grade"].map(grades_priority)

    final_df.sort_values(
        by=["operation_order", "starting_grade_order", "target_grade_order"],
        ascending=[True, True, True],
        inplace=True,
    )

    return final_df.to_dict("records")


def learners_progress_layout():
    return html.Div(
        [
            html.H1("Learners Progress Dashboard"),
            dcc.Dropdown(
                id="dig-l-prog-schools-dropdown",
                placeholder="Select School",
                options=get_school_options(),
                style={"width": "300px", "margin": "10px"},
            ),
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
                            "whiteSpace": "pre-wrap",
                            "height": "auto",
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
                        },
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
                                    "backgroundColor": operation_color_coding[
                                        operation
                                    ],
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


layout = learners_progress_layout
