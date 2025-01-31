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

# Fetch distinct question set types from the database
qset_types = pd.read_sql("SELECT DISTINCT(purpose) FROM question_set", engine)

# Fetch grades from the database
grades = pd.read_sql("SELECT id, cm.name->>'en' AS grade FROM class_master cm", engine)

# Define operation priorities for sorting
operations_priority = {
    "Addition": 0,
    "Subtraction": 1,
    "Multiplication": 2,
    "Division": 3,
}

# Map grades to their priorities for sorting
grades_priority = grades.set_index("grade").to_dict().get("id")


@callback(
    Output("dig-qgp-data-table", "data"), Input("dig-qgp-qset-types-dropdown", "value")
)
def update_table(selected_sheet_type):
    # Filter query based on selected question set type
    sheet_type_filter = ""
    if selected_sheet_type:
        sheet_type_filter = f"AND qs.purpose = '{selected_sheet_type}'"

    # Query to fetch detailed question set level data
    qset_level_data_query = f"""
    SELECT lpd.taxonomy->'l1_skill'->'name'->'en' AS operation,
        lpd.taxonomy->'class'->'name'->'en' AS qset_grade,
        lpd.learner_id,
        lpd.question_set_id,
        qs.title->'en' AS qset_name,
        lpd.score
    FROM learner_journey lj
    LEFT JOIN learner_proficiency_question_set_level_data lpd ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN question_set qs ON qs.identifier = lj.question_set_id
    WHERE lj.status='completed' AND ( lpd.score < 0.2 OR lpd.score > 0.9 ) {sheet_type_filter}
    """
    qset_level_data = pd.read_sql(qset_level_data_query, engine)

    # Query to fetch aggregated question set level data
    qset_level_agg_query = f"""
    SELECT lpd.taxonomy->'l1_skill'->'name'->'en' AS operation,
        lpd.taxonomy->'class'->'name'->'en' AS qset_grade,
        COUNT(*) AS attempts_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lpd.score) AS median,
        avg(lpd.score)
    FROM learner_journey lj
    LEFT JOIN learner_proficiency_question_set_level_data lpd ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN question_set qs ON qs.identifier = lj.question_set_id
    WHERE lj.status='completed' {sheet_type_filter}
    GROUP BY lpd.taxonomy->'l1_skill'->'name'->'en', lpd.taxonomy->'class'->'name'->'en'
    """
    qset_level_agg_data = pd.read_sql(qset_level_agg_query, engine)

    # Group data based on score thresholds
    qset_data_based_on_score = (
        qset_level_data.groupby(["operation", "qset_grade"])
        .agg(
            qsets_score_less_than_20=(
                "qset_name",
                lambda x: ", \n".join(
                    map(str, x[qset_level_data["score"] < 0.2].sort_values().unique())
                ),
            ),
            qsets_score_more_than_90=(
                "qset_name",
                lambda x: ", \n".join(
                    map(str, x[qset_level_data["score"] > 0.9].sort_values().unique())
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

    # Map operation and grade to their order for sorting
    final_df.loc[:, "operation_order"] = final_df["operation"].map(operations_priority)
    final_df.loc[:, "qset_grade_order"] = final_df["qset_grade"].map(grades_priority)

    # Sort the dataframe by operation and grade order
    final_df.sort_values(
        by=["operation_order", "qset_grade_order"],
        ascending=[True, True],
        inplace=True,
    )

    # Return the final dataframe as a dictionary for the Dash DataTable
    return final_df.to_dict("records")


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
        html.H1("Question-Set Grade Level Performance Dashboard"),
        # Dropdown for selecting the question set type
        dcc.Dropdown(
            id="dig-qgp-qset-types-dropdown",
            options=[
                {"label": qset_type, "value": qset_type}
                for qset_type in qset_types["purpose"].sort_values().unique()
            ],
            placeholder="Select Question Set Type",
            style={"width": "300px", "margin": "10px"},
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


# if __name__ == "__main__":
#     app.run_server(debug=True)
