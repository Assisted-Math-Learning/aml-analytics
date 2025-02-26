import dash
import pandas as pd

from dash import Dash, Input, Output, callback, dash_table, dcc, html

from db_utils import (
    ALL_GRADES_KEY,
    ALL_QSET_TYPES_KEY,
    ALL_REPOSITORY_NAMES_KEY,
    get_data,
    get_qset_agg_data,
    get_qset_score_data,
)


# Register the page with Dash
dash.register_page(__name__)

""" UTILS """
# Define operation priorities for sorting
operations_priority = {
    "Addition": 0,
    "Subtraction": 1,
    "Multiplication": 2,
    "Division": 3,
}

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

###################################  Digital Grade Performance Dashboard Logic ###################################


@callback(
    Output("dig-qgp-data-table", "data"),
    Output("dig-qgp-repo-dropdown", "options"),
    Output("dig-qgp-qset-types-dropdown", "options"),
    Input("dig-qgp-repo-dropdown", "value"),
    Input("dig-qgp-qset-types-dropdown", "value"),
)
def update_table(selected_repo, selected_sheet_type):
    # Query to fetch detailed question set level data
    # The query retrieves detailed data on question sets with extreme scores (below 0.2 or above 0.9) for completed learner journeys, optionally filtered by a specific question set type.
    # It joins data from learner proficiency and question set tables to extract operation, grade, learner ID, question set ID, name, and score.
    qset_level_data = get_qset_score_data(selected_sheet_type, selected_repo)

    # Query to fetch aggregated question set level data
    # The query aggregates question set data for completed learner journeys, calculating the number of attempts, median score, and average score, grouped by operation and grade.
    # It optionally filters results based on a selected question set type.
    qset_level_agg_data = get_qset_agg_data(selected_sheet_type, selected_repo)

    # Group data based on score thresholds
    # It groups question set data by operation and grade, aggregating names of question sets with scores below 0.2 and above 0.9 into comma-separated strings.
    qset_data_based_on_score = (
        qset_level_data.groupby(["operation", "qset_grade"])
        .agg(
            qsets_score_less_than_20=(
                "qset_name",
                lambda x: ", \n".join(
                    map(
                        str,
                        x[qset_level_data["avg_score"] < 0.2].sort_values().unique(),
                    )
                ),
            ),
            qsets_score_more_than_90=(
                "qset_name",
                lambda x: ", \n".join(
                    map(
                        str,
                        x[qset_level_data["avg_score"] > 0.9].sort_values().unique(),
                    )
                ),
            ),
        )
        .reset_index()
    )

    # Merge aggregated data with score-based data
    final_df = qset_level_agg_data.merge(
        qset_data_based_on_score, how="left", on=["operation", "qset_grade"]
    )

    # Format median and average scores as percentages
    final_df["median"] = final_df["median"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )
    final_df["avg"] = final_df["avg"].apply(
        lambda x: f"{round(x*100)} %" if pd.notna(x) else x
    )

    # Fetch grades
    grades = get_data(ALL_GRADES_KEY)
    # Map grades to their priorities for sorting
    grades_priority = grades.set_index("grade").to_dict().get("id")

    # Map operation and grade to their order for sorting
    final_df.loc[:, "operation_order"] = final_df["operation"].map(operations_priority)
    final_df.loc[:, "qset_grade_order"] = final_df["qset_grade"].map(grades_priority)

    # Sort the dataframe by operation and grade order
    final_df.sort_values(
        by=["operation_order", "qset_grade_order"],
        ascending=[True, True],
        inplace=True,
    )

    # Repository dropdown options
    repo_options = [
        {"label": repo, "value": repo} for repo in get_data(ALL_REPOSITORY_NAMES_KEY)
    ]

    # Qset type dropdown options
    qset_type_options = [
        {"label": qset_type, "value": qset_type}
        for qset_type in get_data(ALL_QSET_TYPES_KEY)
    ]

    # Return the final dataframe as a dictionary for the Dash DataTable
    return final_df.to_dict("records"), repo_options, qset_type_options


###################################  Digital Grade Performance Dashboard Layout ###################################

# Define the layout of the Dash app
layout = html.Div(
    [
        html.H1("Question-Set Grade Level Performance Dashboard"),
        html.Div(
            [
                # Dropdown for selecting repository name
                dcc.Dropdown(
                    id="dig-qgp-repo-dropdown",
                    placeholder="Select Repository",
                    style={"width": "300px", "margin": "10px"},
                ),
                # Dropdown for selecting the question set type
                dcc.Dropdown(
                    id="dig-qgp-qset-types-dropdown",
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
        dcc.Loading(
            id="dig-qgp-loading-table",
            type="circle",
            children=[
                dash_table.DataTable(
                    id="dig-qgp-data-table",
                    columns=[
                        {"name": "Operation", "id": "operation"},
                        {"name": "Class", "id": "qset_grade"},
                        {"name": "Attempt Count", "id": "attempts_count"},
                        {"name": "Median Accuracy", "id": "median"},
                        {"name": "Average Accuracy", "id": "avg"},
                        {
                            "name": "Difficult Question Sets",
                            "id": "qsets_score_less_than_20",
                        },
                        {
                            "name": "Easy Question Sets",
                            "id": "qsets_score_more_than_90",
                        },
                    ],
                    style_table={
                        "overflowX": "auto",
                        "overflowY": "auto",
                        "maxWidth": "100%",
                        "marginTop": "20px",
                        "marginBottom": "50px",
                        "maxHeight": "800px",
                    },
                    style_data={
                        "whiteSpace": "nowrap",  # Allow content to wrap
                        "overflow": "auto",  # Prevent cell expansion
                        # "textOverflow": "ellipsis",  # Show ellipsis for trimmed content
                        "minHeight": "100px",  # Limit cell height
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
                        "maxWidth": "150px",  # Fixed max width for cells
                        "minHeight": "100px",  # Fixed height for cells
                        "overflow": "auto",  # Scrollbars for overflow
                        "whiteSpace": "nowrap",  # Allow line wrapping
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
                                    "filter_query": f"{{qset_grade}} = '{grade}'",
                                    "column_id": f"qset_grade",
                                },
                                "backgroundColor": grade_color_coding[grade],
                            }
                            for grade in grade_color_coding.keys()
                        ),
                    ],
                    css=[
                        {
                            "selector": ".dash-spreadsheet tr td",
                            "rule": "height: 75px;",
                        },  # set height of body rows
                    ],
                )
            ],
        ),
    ]
)
