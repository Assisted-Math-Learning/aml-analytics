import dash
from db_utils import (
    ALL_GRADES_KEY,
    ALL_LEARNER_DATA_KEY,
    ALL_LEARNERS_KEY,
    ALL_QSET_TYPES_KEY,
    ALL_SCHOOLS_KEY,
    get_data,
    get_min_max_timestamp,
    get_question_sequence_data,
    last_synced_time,
)
import numpy as np
import pandas as pd
import pytz
import re
from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from datetime import datetime, timedelta

# Register the page
# app = Dash(__name__)
# server = app.server
dash.register_page(__name__)


""" UTILS """
# Define IST timezone
IST = pytz.timezone("Asia/Kolkata")


# Function to check the format of columns as week-range column
def check_column_format(input_column):
    # Define the regular expression pattern for the format 'YYYY-MM-DD,YYYY-MM-DD %'
    pattern = r"\d{4}-\d{2}-\d{2},\d{4}-\d{2}-\d{2}"
    # Check if the column matches the pattern
    if re.search(pattern, input_column):
        return True
    return False


# Generate dropdown options for week ranges
def generate_week_ranges(from_date, to_date):
    # Set the min and max dates
    min_date = from_date
    max_date = to_date

    # Adjust min_date to the start of the week (Monday)
    start_date = min_date - timedelta(days=min_date.weekday())

    # Adjust max_date to the end of the week (Sunday)
    end_date = max_date + timedelta(days=(6 - max_date.weekday()))

    # Generate week ranges from start_date to end_date
    week_ranges = []
    while end_date >= start_date:
        week_end = end_date
        week_start = week_end - timedelta(days=(6))

        week_ranges.append(
            {
                "name": f"{week_start.strftime('%d %b')} to {week_end.strftime('%d %b %Y')}",
                "id": f"{week_start.strftime('%Y-%m-%d')},{week_end.strftime('%Y-%m-%d')}",
            }
        )

        # Move to the next week
        end_date -= timedelta(days=(7))

    return week_ranges


def calculate_range(date):
    week_start = date - timedelta(days=date.weekday())
    week_end = week_start + timedelta(days=6)
    return f"{week_start.strftime('%Y-%m-%d')},{week_end.strftime('%Y-%m-%d')}"


operations_priority = {
    "Addition": 0,
    "Subtraction": 1,
    "Multiplication": 2,
    "Division": 3,
}


# Define the logic for applying diff based on the sum of 'time_diff_clipped'
def apply_diff_or_copy(group):
    if group["time_diff_clipped"].sum() > 2700:
        # Apply diff only if sum of 'time_diff_clipped' > threshold
        group["time_diff_clipped"] = group["time_diff_clipped"].diff(1).abs()
    else:
        # If sum is <= threshold, keep 'time_diff_clipped' values
        group["time_diff_clipped"] = group["time_diff_clipped"]
    return group


def get_all_learners_options(school: str = ""):
    """Fetch all unique learner IDs from the database."""
    learners_df = get_data(ALL_LEARNERS_KEY)

    # Add school filter if provided
    if school == "No School":
        learners_df = learners_df[learners_df["school"].isnull()]
    elif school:
        learners_df = learners_df[learners_df["school"] == school]

    learners_df = learners_df.sort_values(["user_name"], ascending=[True])
    learners_df["learners_name"] = learners_df["user_name"].str.cat(
        learners_df["name"], sep="-", na_rep=""
    )
    return learners_df["learners_name"].unique()


@callback(
    [
        Output("dig-li-learner-dropdown", "options"),
        Output("dig-li-learner-dropdown", "value"),
    ],
    Input("dig-li-schools-dropdown", "value"),
)
def update_learners_options(selected_school: str):
    """Update the options for the learners dropdown based on selected school."""
    all_learners = (
        get_all_learners_options(selected_school)
        if selected_school
        else get_all_learners_options()
    )
    return [{"label": learner, "value": learner} for learner in all_learners], ""


# Learners data is basically the data of all learners attempts of all their question-sets and their questions


print("I am here")


# Create Grade Jump Data
def get_grade_jump_data(grade_jump_data):
    # grade_jump_data = all_learners_data.copy()

    # Filtering the data as per the purpose column
    grade_jump_data = grade_jump_data[grade_jump_data["purpose"] != "Main Diagnostic"]

    # If the filtered data is empty, handle accordingly
    if not grade_jump_data.empty:
        grade_jump_data.loc[:, "date"] = grade_jump_data["updated_at"].dt.date

        # Aggregating data
        grade_jump_data = (
            grade_jump_data.groupby(
                ["learner_id", "school", "grade", "operation", "qset_grade", "date"]
            )
            .agg(min_time=("updated_at", "min"), max_time=("updated_at", "max"))
            .reset_index()
        )

        grade_jump_data["time_diff"] = (
            grade_jump_data["max_time"] - grade_jump_data["min_time"]
        ).dt.total_seconds()

        # Set maximum value of 'time_diff' to 45 minutes = 2700 seconds
        grade_jump_data["time_diff_clipped"] = grade_jump_data["time_diff"].clip(
            upper=2700
        )
        grade_jump_data.sort_values(
            by=["learner_id", "date", "min_time"],
            ascending=[True, True, True],
            inplace=True,
        )

        # Apply the diff function to each group of 'learner_id' and 'date'
        grade_jump_data = grade_jump_data.groupby(["learner_id", "date"]).apply(
            apply_diff_or_copy
        )
        grade_jump_data.reset_index(drop=True, inplace=True)
        grade_jump_data["time_diff_clipped"] = grade_jump_data[
            "time_diff_clipped"
        ].fillna(grade_jump_data["time_diff"])
        grade_jump_data.drop(columns=["time_diff"], inplace=True)
        grade_jump_data.rename(columns={"time_diff_clipped": "time_diff"}, inplace=True)
        grade_jump_data.loc[:, "operation_order"] = grade_jump_data["operation"].map(
            operations_priority
        )
        # Fetch grades
        grades = get_data(ALL_GRADES_KEY)
        # Map grades to their priorities for sorting
        grades_priority = grades.set_index("grade").to_dict().get("id")
        grade_jump_data.loc[:, "grade_order"] = grade_jump_data["qset_grade"].map(
            grades_priority
        )
    return grade_jump_data


# Create Operator Jump Data
def get_operator_jump_data(operator_jump_data):
    # operator_jump_data = all_learners_data.copy()

    if not operator_jump_data.empty:
        operator_jump_data.loc[:, "date"] = operator_jump_data["updated_at"].dt.date

        # Aggregating data
        operator_jump_data = (
            operator_jump_data.groupby(
                ["learner_id", "school", "grade", "operation", "date"]
            )
            .agg(min_time=("updated_at", "min"), max_time=("updated_at", "max"))
            .reset_index()
        )

        operator_jump_data["time_diff"] = (
            operator_jump_data["max_time"] - operator_jump_data["min_time"]
        ).dt.total_seconds()
        # Set maximum value of 'time_diff' to 45 minutes = 2700 seconds
        operator_jump_data["time_diff_clipped"] = operator_jump_data["time_diff"].clip(
            upper=2700
        )
        operator_jump_data.sort_values(
            by=["learner_id", "date", "min_time"],
            ascending=[True, True, True],
            inplace=True,
        )

        # Apply the diff function to each group of 'learner_id' and 'date'
        operator_jump_data = operator_jump_data.groupby(["learner_id", "date"]).apply(
            apply_diff_or_copy
        )
        operator_jump_data.reset_index(drop=True, inplace=True)
        operator_jump_data["time_diff_clipped"] = operator_jump_data[
            "time_diff_clipped"
        ].fillna(operator_jump_data["time_diff"])
        operator_jump_data.drop(columns=["time_diff"], inplace=True)
        operator_jump_data.rename(
            columns={"time_diff_clipped": "time_diff"}, inplace=True
        )
        operator_jump_data.loc[:, "operation_order"] = operator_jump_data[
            "operation"
        ].map(operations_priority)

    return operator_jump_data


###################################  Digital Master Dashboard Logic ###################################

# NOTE: We are displaying data in two time ranges - 1. OVERALL 2. WEEK WISE
# OVERALL: By default, It displays the metrics calculated on all the data but it updates with filters applied.
# WEEK WISE: By default, It displays the metrics calculated at week level for last 6 weeks but the weeks updates based on selected date range.


""" UNIQUE LEARNERS """
########################## - OVERALL


# Q: What are overall unique learners?
# - All the learners who have attempted/solved even a single question on digital app are overall unique learners
# - There are sub-divisions of unique learners based on operation - 'Addition', 'Subtraction', 'Multiplication', 'Division'
# - These subdivisions tell that how many unique learners have attempted questions of these operations till now
def get_overall_unique_learners(
    uni_learners_df,
    from_date: str,
    to_date: str,
    school: str,
    grade: str,
    operation: str,
):
    # Create a copy of the all_learners_data DataFrame
    # uni_learners_df = all_learners_data.copy()
    print("Shape of unique_learners: ", uni_learners_df.shape)

    # Filter the DataFrame based on the provided date range if both from_date and to_date are provided
    if from_date and to_date:
        uni_learners_df = uni_learners_df[
            (uni_learners_df["updated_at"].dt.date >= from_date)
            & (uni_learners_df["updated_at"].dt.date <= to_date)
        ]

    # Apply school filter if 'school' is provided
    if school:
        uni_learners_df = uni_learners_df[uni_learners_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        uni_learners_df = uni_learners_df[uni_learners_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        uni_learners_df = uni_learners_df[uni_learners_df["operation"] == operation]

    # Count distinct learners
    overall_count = uni_learners_df["learner_id"].nunique()
    # Create a DataFrame to hold the overall count
    overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    # Group the DataFrame by 'operation' and count unique learners for each operation
    op_wise_uni_learners_df = (
        uni_learners_df.groupby(["operation"])["learner_id"]
        .nunique()
        .reset_index(name="overall_count")
    )
    # Define a list of operations to consider
    operators_ls = ["Addition", "Subtraction", "Multiplication", "Division"]
    # Reindex the operation-wise unique learners DataFrame to include all operations, even if they have no learners
    op_wise_uni_learners_df = op_wise_uni_learners_df.set_index("operation").reindex(
        operators_ls, fill_value=pd.NA
    )
    # Reset the index to remove the operation as index
    op_wise_uni_learners_df = op_wise_uni_learners_df.reset_index(drop=True)
    # Concatenate the overall count DataFrame with the operation-wise unique learners DataFrame
    overall_uni_learners_df = pd.concat(
        [overall_count_df, op_wise_uni_learners_df]
    ).reset_index(drop=True)

    return overall_uni_learners_df


########################## - WEEK WISE


# The definition of unique learners is same as above. The difference is that here the data will be generated for per week.
# The Number of learners solved/attempted even a single question in that week.
def get_unique_learners(
    all_learners_data, from_date, to_date, school, grade, operation
):
    # Fetch overall unique learners count
    overall_unique_learners = get_overall_unique_learners(
        all_learners_data, from_date, to_date, school, grade, operation
    )

    # Create a copy of all learners data
    weekly_unique_learners = all_learners_data.copy()

    # Filter learners records after from_date
    if from_date:
        weekly_unique_learners = weekly_unique_learners[
            weekly_unique_learners["updated_at"].dt.date >= from_date
        ]

    # Filter learners records till to_date
    if to_date:
        weekly_unique_learners = weekly_unique_learners[
            weekly_unique_learners["updated_at"].dt.date <= to_date
        ]

    # Filter learners of selected school
    if school:
        weekly_unique_learners = weekly_unique_learners[
            weekly_unique_learners["school"] == school
        ]

    # Filter learners of selected grade
    if grade:
        weekly_unique_learners = weekly_unique_learners[
            weekly_unique_learners["grade"] == grade
        ]
    # Filter learners who have performed selected operation
    if operation:
        weekly_unique_learners = weekly_unique_learners[
            weekly_unique_learners["operation"] == operation
        ]

    if not weekly_unique_learners.empty:
        # Get unique learners active on every date
        weekly_unique_learners = weekly_unique_learners.drop_duplicates(
            subset=["learner_id", "operation", "updated_date"]
        )

        # Map week range to every entry based on date
        weekly_unique_learners.loc[:, "week_range"] = weekly_unique_learners[
            "updated_date"
        ].apply(lambda x: calculate_range(x))

        # Get unique learners active within a week
        final_unique_learners_df = weekly_unique_learners.drop_duplicates(
            subset=["learner_id", "operation", "week_range"]
        ).copy()

        # Rename 'learner_id' to active learners
        final_unique_learners_df.rename(
            columns={"learner_id": "active_learners"}, inplace=True
        )

        # Create pivot table for weekly representation of unique learners count
        weekly_uni_lrs_cnt_table = pd.pivot_table(
            final_unique_learners_df,
            columns="week_range",  # columns
            values="active_learners",  # Values to aggregate
            aggfunc="nunique",  # Aggregation function
        ).reset_index(names=["metrics"])

        weekly_uni_lrs_cnt_table.loc[0, "metrics"] = "active learners"
    else:
        weekly_uni_lrs_cnt_table = pd.DataFrame({"metrics": ["active learners"]})
        final_unique_learners_df = pd.DataFrame(
            columns=["active_learners", "operation", "updated_date", "week_range"]
        )

    operators_ls = ["Addition", "Subtraction", "Multiplication", "Division"]
    if not final_unique_learners_df.empty:
        # Create pivot table for weekly representation of operation wise unique learners count
        op_wise_weekly_uni_lrs_cnt_table = pd.pivot_table(
            final_unique_learners_df,
            index="operation",
            columns="week_range",  # columns
            values="active_learners",  # Values to aggregate
            aggfunc="nunique",  # Aggregation function
        )

        op_wise_weekly_uni_lrs_cnt_table = op_wise_weekly_uni_lrs_cnt_table.reindex(
            operators_ls, fill_value=pd.NA
        )
        op_wise_weekly_uni_lrs_cnt_table = (
            op_wise_weekly_uni_lrs_cnt_table.reset_index().rename(
                columns={"operation": "sub_metrics"}
            )
        )
    else:
        op_wise_weekly_uni_lrs_cnt_table = pd.DataFrame(
            {"sub_metrics": ["Addition", "Subtraction", "Multiplication", "Division"]}
        )

    weekly_uni_lrs_cnt_table = pd.concat(
        [weekly_uni_lrs_cnt_table, op_wise_weekly_uni_lrs_cnt_table]
    )

    return overall_unique_learners, weekly_uni_lrs_cnt_table, final_unique_learners_df


""" NEW LEARNERS ADDED """
########################## - WEEK WISE


# Q: What are new learners added?
# - The number of learners who have started attempting questions on digital app from that week.
# - That's why this data is based on weeks. It simply calculates the learners record found till that week.
# - It signifies learners who were already enrolled.
# - And, Subtract those learners from learners having records in that week. This will provide the new learners if added that week.
def get_new_learners_added(final_unique_learners_df, previous_learners_list):
    # Create a copy of the filtered unique learners data to avoid modifying the original DataFrame
    learners_added_df = final_unique_learners_df.copy()

    # Sort the data based on the 'updated_date' column to ensure chronological order
    learners_added_df = learners_added_df.sort_values("updated_date")

    # Initialize a set to track learner_ids seen in previous weeks for efficient lookup
    previous_learners = set(previous_learners_list)

    # Define a function to calculate the count of new learners for a given week
    def count_new_learners_for_week(current_week, current_learners):
        # Identify learners in the current week who have not appeared in previous weeks
        new_learners = [
            learner for learner in current_learners if learner not in previous_learners
        ]
        # Update the 'previous_learners' set with the current week's learners to keep track of all seen learners
        previous_learners.update(current_learners)
        return len(new_learners)

    # Apply the 'count_new_learners_for_week' function to each week in the DataFrame to calculate new learners added
    learners_added_df["new learners added"] = learners_added_df.groupby("week_range")[
        "active_learners"
    ].transform(lambda x: count_new_learners_for_week(x.name, x.tolist()))

    # Use a pivot table to reshape the data and get the count of new learners added per week
    weekly_new_learners_added = learners_added_df.pivot_table(
        values="new learners added", columns="week_range"
    ).reset_index(names=["metrics"])

    return weekly_new_learners_added


""" SESSIONS COUNT """
########################## - OVERALL


# Q: What is the definition of session?
# - If we have records of 3 or more learners of a class of a school an a date, then that will be counted as a session.
def get_overall_sessions(
    uni_sessions_df, from_date: str, to_date: str, school: str, grade: str
):
    # Create a copy of the all_learners_data DataFrame to work with
    # uni_sessions_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        # Filter the DataFrame to include only records within the specified date range
        uni_sessions_df = uni_sessions_df[
            (uni_sessions_df["updated_at"].dt.date >= from_date)
            & (uni_sessions_df["updated_at"].dt.date <= to_date)
        ]

    # Filter learners of selected school
    if school:
        uni_sessions_df = uni_sessions_df[uni_sessions_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        # Filter the DataFrame to include only records of the specified grade
        uni_sessions_df = uni_sessions_df[uni_sessions_df["grade"] == grade]

    # Check if the DataFrame is empty after applying filters
    if uni_sessions_df.empty:
        # If the DataFrame is empty, return a DataFrame with an overall count of 0
        overall_count_df = pd.DataFrame([{"overall_count": 0}])
    else:
        # Group the DataFrame by date and grade, and count the number of distinct learners (learner_id)
        session_groups = (
            uni_sessions_df.groupby(
                [uni_sessions_df["updated_at"].dt.date, "school", "grade"]
            )
            .agg(total_learners=("learner_id", "nunique"))
            .reset_index()
        )

        # Filter out groups where the number of distinct learners is less than 3
        session_groups = session_groups[session_groups["total_learners"] >= 3]

        # Check if the DataFrame is empty after filtering out groups with less than 3 learners
        if session_groups.empty:
            # If the DataFrame is empty after filtering, return a DataFrame with an overall count of 0
            overall_count_df = pd.DataFrame([{"overall_count": 0}])
        else:
            # Count the number of remaining sessions (groups with 3 or more learners)
            overall_count = session_groups.shape[0]

            # Create the final result as a DataFrame with the overall count
            overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    # Return the DataFrame containing the overall count of sessions
    return overall_count_df


########################## - WEEK WISE


# - This will provide the number of sessions conducted in that week.
def get_sessions(all_learners_data, from_date, to_date, school, grade):
    # Fetch the overall sessions count
    overall_sessions = get_overall_sessions(
        all_learners_data, from_date, to_date, school, grade
    )

    # Create a copy of the all learners data DataFrame
    weekly_sessions_data = all_learners_data.copy()

    # Filter learners records to include only those after the specified from_date
    if from_date:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["updated_at"].dt.date >= from_date
        ]

    # Filter learners records to include only those up to the specified to_date
    if to_date:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["updated_at"].dt.date <= to_date
        ]

    # Filter learners of selected school
    if school:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["school"] == school
        ]

    # Filters learners of the selected grade
    if grade:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["grade"] == grade
        ]

    # Check if the DataFrame is not empty after applying filters
    if not weekly_sessions_data.empty:
        # Get unique learners of respective grades on every date
        weekly_sessions_data = weekly_sessions_data.drop_duplicates(
            subset=["school", "grade", "updated_date", "learner_id"]
        )

        # Map week range to every entry based on date
        weekly_sessions_data.loc[:, "week_range"] = weekly_sessions_data[
            "updated_date"
        ].apply(lambda x: calculate_range(x))

        # Count the number of unique learners of respective grades on every date
        grouped_session_data = (
            weekly_sessions_data.groupby(
                ["school", "grade", "week_range", "updated_date"]
            )["learner_id"]
            .count()
            .reset_index()
        )

        # If the number of unique learners for any grade on any date is equal or greater than 3,
        # then mark that as a session = 1 else 0
        grouped_session_data["sessions"] = (
            grouped_session_data["learner_id"] >= 3
        ).astype("int")

        # Create a pivot table for weekly representation of sessions count
        weekly_sessions_cnt_table = pd.pivot_table(
            grouped_session_data, values="sessions", columns="week_range", aggfunc="sum"
        ).reset_index(names=["metrics"])
    else:
        # If the DataFrame is empty after filtering, return a DataFrame with a single column 'metrics'
        weekly_sessions_cnt_table = pd.DataFrame({"metrics": ["sessions"]})

    return overall_sessions, weekly_sessions_cnt_table


""" WORK DONE """
########################## - OVERALL


# Q: What is the definition of work done?
# - The number of questions is simply called as work done.
# - overall work done is total number of questions solved by all learners on the digital app till now.
def get_overall_work_done(
    work_done_df, from_date: str, to_date: str, school: str, grade: str, operation: str
):
    # Create a copy of the all_learners_data DataFrame to work with
    # work_done_df = all_learners_data.copy()

    # Apply date filter if both 'from_date' and 'to_date' are provided
    if from_date and to_date:
        # Filter the DataFrame to include only records within the specified date range
        work_done_df = work_done_df[
            (work_done_df["updated_at"].dt.date >= from_date)
            & (work_done_df["updated_at"].dt.date <= to_date)
        ]

    # Filter learners of selected school
    if school:
        work_done_df = work_done_df[work_done_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        # Filter the DataFrame to include only records of the specified grade
        work_done_df = work_done_df[work_done_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        # Filter the DataFrame to include only records of the specified operation
        work_done_df = work_done_df[work_done_df["operation"] == operation]

    # Check if the DataFrame is empty after applying all filters
    if work_done_df.empty:
        # If the DataFrame is empty, return a DataFrame with an overall count of 0
        overall_count_df = pd.DataFrame([{"overall_count": 0}])
    else:
        # If the DataFrame is not empty, count the number of distinct learners
        overall_count = work_done_df.shape[0]

        # Create a DataFrame to hold the overall count
        overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    # Return the DataFrame containing the overall count
    return overall_count_df


########################## - WEEK WISE


# This will provide the number of questions solved by learners in respective weeks.
def get_work_done(
    all_learners_data,
    from_date: str,
    to_date: str,
    school: str,
    grade: str,
    operation: str,
):
    # Fetch the overall work done based on the provided filters
    overall_work_done = get_overall_work_done(
        all_learners_data, from_date, to_date, school, grade, operation
    )

    # Create a copy of the all_learners_data DataFrame to work with
    work_done_df = all_learners_data.copy()

    # Apply date filter if 'from_date' is provided
    if from_date:
        # Filter the DataFrame to include only records with dates on or after 'from_date'
        work_done_df = work_done_df[work_done_df["updated_at"].dt.date >= from_date]

    # Apply date filter if 'to_date' is provided
    if to_date:
        # Filter the DataFrame to include only records with dates on or before 'to_date'
        work_done_df = work_done_df[work_done_df["updated_at"].dt.date <= to_date]

    # Filter learners of selected school
    if school:
        work_done_df = work_done_df[work_done_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        # Filter the DataFrame to include only records of the specified 'grade'
        work_done_df = work_done_df[work_done_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        # Filter the DataFrame to include only records of the specified 'operation'
        work_done_df = work_done_df[work_done_df["operation"] == operation]

    # Check if the DataFrame is empty after applying all filters
    if not work_done_df.empty:
        # Map the week range to every entry based on the date
        work_done_df.loc[:, "week_range"] = work_done_df["updated_date"].apply(
            lambda x: calculate_range(x)
        )

        # Rename the 'attempts_count' column to 'work done'
        work_done_df.rename(columns={"attempts_count": "work done"}, inplace=True)

        # Create a pivot table for the weekly representation of work done
        weekly_work_done = pd.pivot_table(
            work_done_df, values="work done", columns="week_range", aggfunc="count"
        ).reset_index(names=["metrics"])
    else:
        # If the DataFrame is empty, create a DataFrame with a single column 'metrics' containing 'work done'
        weekly_work_done = pd.DataFrame({"metrics": ["work done"]})
        # Create an empty DataFrame with the required columns
        work_done_df = pd.DataFrame(
            columns=[
                "work done",
                "learner_id",
                "week_range",
            ]
        )

    # Return the overall work done, weekly work done, and the filtered DataFrame
    return overall_work_done, weekly_work_done, work_done_df


""" AVERAGE WORK DONE PER LEARNER """
########################## - OVERALL
########################## - WEEK WISE


# Q: What is the definition of average work done per learner?
# - The number of questions solved by each learner on average is defined as average work done per learner.
# - At overall level, It represents the total average number of questions solved by each learner.
# - At week wise level, It represents the average number of questions solved by learners in that week.
def get_avg_work_done_per_learner(
    work_done_df, overall_work_done, overall_unique_learners
):
    # Calculate the average work done per learner at the overall level
    # This is done by dividing the total work done by the number of unique learners
    overall_learners_count = overall_unique_learners.loc[0, "overall_count"]

    if pd.isna(overall_learners_count) or overall_learners_count == 0:
        overall_work_done_per_lr = 0  # Assign NaN or another appropriate default value
    else:
        overall_work_done_per_lr = (
            overall_work_done.loc[0, "overall_count"] // overall_learners_count
        )

    # Create a DataFrame to hold the overall average work done per learner
    overall_work_done_avg = pd.DataFrame([{"overall_count": overall_work_done_per_lr}])

    # Create a copy of the filtered work done DataFrame to work with
    work_done_per_learner_df = work_done_df.copy()

    # Count the number of unique learners who worked in each week range
    # This is done by mapping the 'week_range' to the count of unique learners for each week range
    work_done_per_learner_df["unique_learners"] = work_done_per_learner_df[
        "week_range"
    ].map(work_done_per_learner_df.groupby("week_range")["learner_id"].nunique())

    # Check if the DataFrame is not empty after adding the 'unique_learners' column
    if not work_done_per_learner_df.empty:
        # Create a pivot table for the weekly representation of work done per learner in that week
        # This pivot table counts the 'work done' and takes the first value of 'unique_learners' for each week range
        weekly_work_done_per_lr = pd.pivot_table(
            work_done_per_learner_df,
            values=["work done", "unique_learners"],
            index="week_range",  # Use 'week_range' as index if it makes sense for your use case
            aggfunc={"work done": "count", "unique_learners": "first"},
        )
        # Calculate the average work done per learner for each week range
        weekly_work_done_per_lr["work done per learner"] = (
            weekly_work_done_per_lr["work done"]
            // weekly_work_done_per_lr["unique_learners"]
        )
        # Select only the 'work done per learner' column and transpose the DataFrame
        weekly_work_done_per_lr = weekly_work_done_per_lr[["work done per learner"]]
        # Reset the index to include 'metrics' as the column name
        weekly_work_done_per_lr = weekly_work_done_per_lr.transpose().reset_index(
            names=["metrics"]
        )
    else:
        # If the DataFrame is empty, create a DataFrame with a single column 'metrics' containing 'work done per learner'
        weekly_work_done_per_lr = pd.DataFrame({"metrics": ["work done per learner"]})
    # Return the overall average work done per learner and the weekly average work done per learner
    return overall_work_done_avg, weekly_work_done_per_lr


""" MEDIAN WORK DONE PER LEARNER """
########################## - OVERALL
########################## - WEEK WISE


# Q: What is the definition of median work done per learner?
# - The central value of number of questions solved by a learner.
# - It will signify the minimum/maximum number of questions getting attempted by half of the learners.
# - At overall level, It represents the median of number of questions attempted by each learner till now.
# - At week wise level, It represents the median of number of questions attempted by learners in that week.
def get_median_work_done_per_learner(work_done_df):
    # Create a copy of the filtered work done data to work with
    work_done_per_learner_df = work_done_df.copy()

    # Calculate the overall median work done per learner by counting the 'work done' for each learner
    overall_median_work_done_per_lr = (
        work_done_per_learner_df.groupby("learner_id")
        .agg(
            work_done=("work done", "count")
        )  # Aggregate 'work done' by counting for each learner
        .reset_index()  # Reset the index to include 'learner_id' as a column
    )
    # Calculate the median of the 'work_done' counts to get the overall median work done per learner
    overall_median_work_done = pd.DataFrame(
        [{"overall_count": overall_median_work_done_per_lr["work_done"].median()}]
    )

    # Check if the DataFrame is not empty after calculating the overall median work done per learner
    if not work_done_per_learner_df.empty:
        # Create a DataFrame to hold the work done data for every learner in their respective week ranges
        weekly_work_done_by_learners = (
            work_done_df.groupby(
                ["learner_id", "week_range"]
            )  # Group by 'learner_id' and 'week_range'
            .agg(
                work_done=("work done", "count")
            )  # Aggregate 'work done' by counting for each learner in each week range
            .reset_index()  # Reset the index to include 'learner_id' and 'week_range' as columns
        )

        # Create a pivot table for the weekly representation of the median work done in each week
        weekly_median_work_done = pd.pivot_table(
            weekly_work_done_by_learners,
            values="work_done",  # Use 'work_done' as the value to pivot
            index="week_range",  # Use 'week_range' as the index if it makes sense for your use case
            aggfunc="median",  # Aggregate 'work_done' by taking the median for each week range
        )
        # Transpose the DataFrame to have 'week_range' as columns and 'metrics' as the index
        weekly_median_work_done = weekly_median_work_done.transpose().reset_index(
            names=["metrics"]
        )
        # Set the 'metrics' value for the first row to "Median Work Done Per Learner"
        weekly_median_work_done.loc[0, "metrics"] = "Median Work Done Per Learner"
    else:
        # If the DataFrame is empty, create a DataFrame with a single column 'metrics' containing 'Median Work Done Per Learner'
        weekly_median_work_done = pd.DataFrame(
            {"metrics": ["Median Work Done Per Learner"]}
        )
    # Return the overall median work done per learner and the weekly median work done per learner
    return overall_median_work_done, weekly_median_work_done


""" TIME TAKEN """
########################## - OVERALL


# Q: What is the definition of time taken?
# - Time taken is defined as the difference of time of first attempted and last attempted question of the learner on that date.
# - But, Aggregating the time taken/spent by all learners on the digital app till date is represented as overall time taken
# NOTE: Maximum limit of time spent for a learner on a date is 45 minutes/ 2700 seconds.
def get_overall_time_taken(
    time_taken_df, from_date: str, to_date: str, school: str, grade: str, operation: str
):
    """
    This function calculates the overall time taken by all learners.
    It applies date, grade, and operation filters if provided.
    """
    # Create a copy of the all learners data to work with
    # time_taken_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        time_taken_df = time_taken_df[
            (time_taken_df["updated_at"].dt.date >= from_date)
            & (time_taken_df["updated_at"].dt.date <= to_date)
        ]

    # Filter learners of selected school
    if school:
        time_taken_df = time_taken_df[time_taken_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        time_taken_df = time_taken_df[time_taken_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        time_taken_df = time_taken_df[time_taken_df["operation"] == operation]

    # Check if DataFrame is empty after operation filter
    if time_taken_df.empty:
        total_time_taken = 0
    else:
        # Group by learner_id and date, then calculate min and max times
        time_taken_df["updated_at"] = pd.to_datetime(time_taken_df["updated_at"])
        time_taken_df_grouped = (
            time_taken_df.groupby(["learner_id", time_taken_df["updated_at"].dt.date])
            .agg(min_time=("updated_at", "min"), max_time=("updated_at", "max"))
            .reset_index()
        )

        # Calculate time difference in seconds
        time_taken_df_grouped["time_diff"] = (
            time_taken_df_grouped["max_time"] - time_taken_df_grouped["min_time"]
        ).dt.total_seconds()

        # Set maximum value of 'time_diff' to 45 minutes = 2700 seconds
        time_taken_df_grouped["time_diff"] = time_taken_df_grouped["time_diff"].clip(
            upper=2700
        )

        # Sum up the time difference and convert to minutes
        total_time_taken = round(time_taken_df_grouped["time_diff"].sum() / 60, 2)

    # Return the overall time taken as a DataFrame
    return pd.DataFrame([{"overall_count": total_time_taken}])


########################## - WEEK WISE


# - Time taken/spent by learners on the digital app in that week.
def get_total_time_taken(
    all_learners_data, from_date, to_date, school, grade, operation
):
    """
    This function calculates the total time taken by learners on the digital app
    within a specified date range, grade, and operation. It also provides a weekly
    breakdown of the total time taken.
    """
    # Fetch overall time taken
    overall_time_taken = get_overall_time_taken(
        all_learners_data, from_date, to_date, school, grade, operation
    )

    # Create a copy of all learners data to work with
    total_time_df = all_learners_data.copy()

    # Apply date filter if 'from_date' is provided
    if from_date:
        total_time_df = total_time_df[total_time_df["updated_at"].dt.date >= from_date]

    # Apply date filter if 'to_date' is provided
    if to_date:
        total_time_df = total_time_df[total_time_df["updated_at"].dt.date <= to_date]

    # Filter learners of selected school
    if school:
        total_time_df = total_time_df[total_time_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        total_time_df = total_time_df[total_time_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        total_time_df = total_time_df[total_time_df["operation"] == operation]

    if not total_time_df.empty:
        # Calculate max and min timestamp of learners on every date
        total_time_df = (
            total_time_df.groupby(["updated_date", "learner_id"])
            .agg(
                min_time=("updated_at", lambda x: x.loc[x.idxmin()]),
                max_time=("updated_at", lambda x: x.loc[x.idxmax()]),
            )
            .reset_index()
        )

        # Calculate total time in seconds by subtracting max and min time
        total_time_df["time_diff"] = (
            total_time_df["max_time"] - total_time_df["min_time"]
        ).dt.total_seconds()

        # Set maximum value of 'time_diff' to 45 minutes = 2700 seconds
        total_time_df["time_diff"] = total_time_df["time_diff"].clip(upper=2700)

        # Map week range to every entry based on date
        total_time_df.loc[:, "week_range"] = total_time_df["updated_date"].apply(
            lambda x: calculate_range(x)
        )

        # Create pivot table for weekly representation of total time taken
        weekly_total_time = pd.pivot_table(
            total_time_df,
            values="time_diff",
            columns="week_range",
            aggfunc=lambda x: round(x.sum() / 60, 2),
        ).reset_index(names=["metrics"])
        weekly_total_time.loc[0, "metrics"] = "total time spent (in min)"
    else:
        weekly_total_time = pd.DataFrame({"metrics": ["total time spent (in min)"]})
        total_time_df = pd.DataFrame(columns=["time_diff", "week_range", "learner_id"])

    return overall_time_taken, weekly_total_time, total_time_df


""" AVERAGE TIME TAKEN PER LEARNER """
########################## - OVERALL
########################## - WEEK WISE


# Q: What is the definition of average time taken per learner?
# - The time spent by each learner on average on digital app to solve questions.
def avg_time_taken_per_learner(
    total_time_df, overall_time_taken, overall_unique_learners
):
    # Calculate the average time taken per learner at the overall level
    # This is done by dividing the total time taken by the number of unique learners
    overall_learners_count = overall_unique_learners.loc[0, "overall_count"]

    if pd.isna(overall_learners_count) or overall_learners_count == 0:
        overall_time_taken_per_lr = 0  # Assign NaN or another appropriate default value
    else:
        overall_time_taken_per_lr = round(
            overall_time_taken.loc[0, "overall_count"] / overall_learners_count, 2
        )

    # Create a DataFrame to hold the overall average time taken per learner
    overall_time_taken_avg = pd.DataFrame(
        [{"overall_count": overall_time_taken_per_lr}]
    )

    # Create a copy of the filtered total time taken data to work with
    time_taken_per_learner_df = total_time_df.copy()

    # Count the number of unique learners who worked in each week range
    # This is done by mapping the 'week_range' to the count of unique learners for each week range
    time_taken_per_learner_df["unique_learners"] = time_taken_per_learner_df[
        "week_range"
    ].map(time_taken_per_learner_df.groupby("week_range")["learner_id"].nunique())

    # Check if the DataFrame is not empty after adding the 'unique_learners' column
    if not time_taken_per_learner_df.empty:
        # Create a pivot table for the weekly representation of time taken per learner in that week
        # This pivot table calculates the total time taken in minutes and counts the unique learners for each week range
        weekly_time_taken_per_lr = pd.pivot_table(
            time_taken_per_learner_df,
            values=["time_diff", "unique_learners"],
            index="week_range",  # Use 'week_range' as index if it makes sense for your use case
            aggfunc={
                "time_diff": lambda x: round(
                    x.sum() / 60, 2
                ),  # Convert seconds to minutes and round to 2 decimal places
                "unique_learners": "first",  # Take the first value of 'unique_learners' for each week range
            },
        )
        # Calculate the average time spent per learner for each week range
        weekly_time_taken_per_lr["average time spent per learner (in min)"] = round(
            weekly_time_taken_per_lr["time_diff"]
            / weekly_time_taken_per_lr["unique_learners"],
            2,
        )
        # Select only the 'average time spent per learner (in min)' column and transpose the DataFrame
        weekly_time_taken_per_lr = weekly_time_taken_per_lr[
            ["average time spent per learner (in min)"]
        ]
        # Reset the index to include 'metrics' as the column name
        weekly_time_taken_per_lr = weekly_time_taken_per_lr.transpose().reset_index(
            names=["metrics"]
        )
    else:
        # If the DataFrame is empty, create a DataFrame with a single column 'metrics' containing 'average time spent per learner (in min)'
        weekly_time_taken_per_lr = pd.DataFrame(
            {"metrics": ["average time spent per learner (in min)"]}
        )

    # Return the overall average time taken per learner and the weekly average time taken per learner
    return overall_time_taken_avg, weekly_time_taken_per_lr


""" MEDIAN ACCURACY """
########################## - OVERALL


# Q: What is the definition of median accuracy?
# - The central value of accuracy (number of correct questions attempted compared to all the questions attempted) by a learner.
# - It will signify the minimum/maximum number of correctness performed by half of the learners.
# - At overall level, It represents the median of accuracies of all learners on the data attempted till now.
def get_overall_median_accuracy(
    accuracy_df, from_date: str, to_date: str, school: str, grade: str, operation: str
):
    # Create a copy of the all_learners_data DataFrame to work with
    # accuracy_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        # Filter the DataFrame to include only records within the specified date range
        accuracy_df = accuracy_df[
            (accuracy_df["updated_at"].dt.date >= from_date)
            & (accuracy_df["updated_at"].dt.date <= to_date)
        ]

    # Filter learners of selected school
    if school:
        accuracy_df = accuracy_df[accuracy_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        # Filter the DataFrame to include only records of the specified grade
        accuracy_df = accuracy_df[accuracy_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        # Filter the DataFrame to include only records of the specified operation
        accuracy_df = accuracy_df[accuracy_df["operation"] == operation]

    # Check if DataFrame is empty after all filters
    if accuracy_df.empty:
        # If the DataFrame is empty, create a DataFrame with the required columns but no data
        median_accuracy_df = pd.DataFrame(columns=["learner_id", "accuracy"])
    else:
        # Calculate accuracy per learner (SUM(score) / COUNT(score)) * 100
        # This is done by grouping the DataFrame by 'learner_id', aggregating 'score' to calculate accuracy,
        # and then resetting the index to include 'learner_id' in the result
        accuracy_df_grouped = (
            accuracy_df.groupby("learner_id")
            .agg(accuracy=("score", lambda x: round((x.sum() / x.count()) * 100, 2)))
            .reset_index()
        )

        # Set result in 'median_accuracy_df'
        median_accuracy_df = accuracy_df_grouped[["learner_id", "accuracy"]]

    # Return a DataFrame containing the median accuracy
    # This is done by calculating the median of 'accuracy' in 'median_accuracy_df' and rounding it to 2 decimal places
    return pd.DataFrame(
        [{"overall_count": round(median_accuracy_df["accuracy"].median(), 2)}]
    )


########################## - WEEK WISE


# - At week wise level, It represents the median of accuracies of all learners on the data attempted in that week.
def get_median_accuracy(
    all_learners_data, from_date, to_date, school, grade, operation
):
    # Fetch the overall median accuracy of learners
    overall_median_accuracy = get_overall_median_accuracy(
        all_learners_data, from_date, to_date, school, grade, operation
    )

    # Create a copy of the all learners data DataFrame to work with
    median_accuracy_df = all_learners_data.copy()

    # Apply date filter if 'from_date' is provided
    if from_date:
        # Filter the DataFrame to include only records with dates on or after 'from_date'
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["updated_at"].dt.date >= from_date
        ]

    # Apply date filter if 'to_date' is provided
    if to_date:
        # Filter the DataFrame to include only records with dates on or before 'to_date'
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["updated_at"].dt.date <= to_date
        ]

    # Filter learners of selected school
    if school:
        median_accuracy_df = median_accuracy_df[median_accuracy_df["school"] == school]

    # Apply grade filter if 'grade' is provided
    if grade:
        # Filter the DataFrame to include only records of the specified 'grade'
        median_accuracy_df = median_accuracy_df[median_accuracy_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        # Filter the DataFrame to include only records of the specified 'operation'
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["operation"] == operation
        ]

    # Check if the DataFrame is not empty after all filters
    if not median_accuracy_df.empty:
        # Map week range to every entry based on date
        median_accuracy_df.loc[:, "week_range"] = median_accuracy_df[
            "updated_date"
        ].apply(lambda x: calculate_range(x))

        # Calculate the accuracy of every learner in respective weeks
        weekly_accuracy_per_learner = (
            median_accuracy_df.groupby(["week_range", "learner_id"])
            .apply(
                lambda x: pd.Series(
                    {
                        "accuracy": round(
                            (x.loc[:, "score"].sum() / x.loc[:, "score"].count()) * 100,
                            2,
                        )
                    }
                )
            )
            .reset_index()
        )

        # Create a pivot table for the weekly representation of median accuracy per learner
        weekly_median_accuracy = pd.pivot_table(
            weekly_accuracy_per_learner,
            values="accuracy",
            columns="week_range",
            aggfunc=lambda x: round(x.median(), 2),
        ).reset_index(names=["metrics"])
        weekly_median_accuracy.loc[0, "metrics"] = "Median Accuracy Of Learners"
    else:
        # If the DataFrame is empty, create a DataFrame with the required columns but no data
        weekly_median_accuracy = pd.DataFrame(
            {"metrics": ["Median Accuracy Of Learners"]}
        )

    # Return the overall median accuracy and the weekly median accuracy
    return overall_median_accuracy, weekly_median_accuracy


""" MEDIAN TIME TAKEN FOR GRADE JUMP (class-one to class-two) """
########################## - OVERALL
########################## - WEEK WISE

# Q: What is the definition of median time taken for grade jump?
# - Every question set (qset) solved by a learner is associated with an operation and a grade, which we can call a "qset-grade".
# - When a learner moves from one qset-grade to the next qset-grade within an operator, it is referred to as a "grade-jump".
# NOTE: - Grade-jumps are not calculated for qsets of type "Main Diagnostic".
# 1. **Overall:**
#    - Median time taken by all learners to complete a grade jump across all qset-grades.

# 2. **Overall Grade Wise (Class-One to Class-Two):**
#    - Median time taken by all learners to jump from Class-One qset-grade to Class-Two qset-grade.

# 3. **Weekly:**
#    - Median time taken by all learners to complete a grade jump within a week.

# 4. **Weekly Grade WIse (Class-One to Class-Two):**
#    - Median time taken by all learners to jump from Class-One qset-grade to Class-Two qset-grade within a week.


def get_median_time_for_grade_jump(
    all_learners_data, from_date, to_date, school, grade, operation
):
    # Create a copy of the grade jump data to work with
    grade_jump_df = get_grade_jump_data(all_learners_data)

    # grade_jump_df = all_grade_jump_data.copy()
    # Filter learners of selected school
    if school:
        grade_jump_df = grade_jump_df[grade_jump_df["school"] == school]

    # Filter the data for the selected grade if specified
    if grade:
        grade_jump_df = grade_jump_df[grade_jump_df["grade"] == grade]

    # Filter the data for the selected operation if specified
    if operation:
        grade_jump_df = grade_jump_df[grade_jump_df["operation"] == operation]

    if not grade_jump_df.empty:
        # Calculate the total time in seconds taken by a learner on a qset-grade of an operation
        overall_grade_jump_df = (
            grade_jump_df.groupby(
                [
                    "learner_id",
                    "operation",
                    "qset_grade",
                    "operation_order",
                    "grade_order",
                ]
            )
            .agg(
                total_time=("time_diff", "sum"),
                min_timestamp=("min_time", "min"),
                max_timestamp=("max_time", "max"),
            )
            .reset_index()
        )

        # Sort every learner's data based on the operator and the qset-grade
        overall_grade_jump_df.sort_values(
            by=["learner_id", "operation_order", "grade_order"],
            ascending=[True, True, True],
            inplace=True,
        )

        # Calculate the previous grade's time
        overall_grade_jump_df["previous_grade_time"] = overall_grade_jump_df.groupby(
            ["learner_id", "operation"]
        )["total_time"].shift(1)
        # Calculate the previous grade
        overall_grade_jump_df["previous_qset_grade"] = overall_grade_jump_df.groupby(
            ["learner_id", "operation"]
        )["qset_grade"].shift(1)

        final_overall_grad_jump_dt = overall_grade_jump_df[
            overall_grade_jump_df["previous_grade_time"].notna()
        ].copy()

        # Filter the data within the selected date range if specified
        if from_date and to_date:
            final_overall_grad_jump_dt = final_overall_grad_jump_dt[
                (final_overall_grad_jump_dt["min_timestamp"].dt.date >= from_date)
                & (final_overall_grad_jump_dt["min_timestamp"].dt.date <= to_date)
            ]

        # Convert time from seconds to minutes
        final_overall_grad_jump_dt["previous_grade_time"] = round(
            final_overall_grad_jump_dt["previous_grade_time"] / 60, 2
        )
    else:
        # If the DataFrame is empty, create a DataFrame with the required columns but no data
        final_overall_grad_jump_dt = pd.DataFrame(
            columns=[
                "learner_id",
                "previous_qset_grade",
                "previous_grade_time",
                "min_timestamp",
                "week_range",
            ]
        )

    # Create a pivot table for the qset-grade-wise representation of median time-taken for grade jump
    grd_wise_overall_median_time = pd.pivot_table(
        final_overall_grad_jump_dt,
        index="previous_qset_grade",
        values=["previous_grade_time", "learner_id"],
        aggfunc={
            "previous_grade_time": lambda x: round(x.median(), 2),
            "learner_id": "nunique",
        },
    )

    if not grd_wise_overall_median_time.empty:
        # Format the median time with the count of learners
        grd_wise_overall_median_time["previous_grade_time"] = (
            grd_wise_overall_median_time["previous_grade_time"].astype(str)
            + " ("
            + grd_wise_overall_median_time["learner_id"].astype(str)
            + ")"
        )

        # Drop the learner_id column as it's now part of the string
        grd_wise_overall_median_time.drop(columns=["learner_id"], inplace=True)
    else:
        # If the pivot table is empty, set the median time to NaN
        grd_wise_overall_median_time["previous_grade_time"] = np.nan

    # Sort qset-grades based on their priority order
    grades_ls = get_data(ALL_GRADES_KEY).sort_values(by="id")["grade"]
    grd_wise_overall_median_time = grd_wise_overall_median_time.reindex(
        grades_ls, fill_value=pd.NA
    )

    # Remove 'class-six' from the index as it's not required
    grd_wise_overall_median_time = grd_wise_overall_median_time[
        grd_wise_overall_median_time.index != "class-six"
    ]

    # Create a DataFrame for the median time taken for grade-jump for a learner both on overall basis and grade-basis
    overall_median_grade_jump_time = pd.DataFrame(
        {
            "overall_count": [
                f"{round(final_overall_grad_jump_dt['previous_grade_time'].median(), 2)} ({ unique_learners if (unique_learners:=final_overall_grad_jump_dt['learner_id'].nunique()) else np.nan})"
            ]
            + grd_wise_overall_median_time["previous_grade_time"].to_list()
        }
    )

    # Remove NaN values from the overall_median_grade_jump_time DataFrame
    overall_median_grade_jump_time.replace("nan (nan)", "", inplace=True)

    # If no date range is selected, filter data for the default range
    if (not final_overall_grad_jump_dt.empty) and not (from_date and to_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        from_date = most_recent_monday - timedelta(days=7 * 5)
        final_overall_grad_jump_dt = final_overall_grad_jump_dt[
            (final_overall_grad_jump_dt["min_timestamp"].dt.date >= from_date.date())
        ]

    if not final_overall_grad_jump_dt.empty:
        # Calculate the week range for each timestamp
        final_overall_grad_jump_dt.loc[:, "week_range"] = final_overall_grad_jump_dt[
            "min_timestamp"
        ].dt.date.apply(lambda x: calculate_range(x))

        # Create a pivot table for the weekly representation of median time-taken for grade jump
        weekly_median_time = (
            pd.pivot_table(
                final_overall_grad_jump_dt,
                columns="week_range",
                values="previous_grade_time",
                aggfunc=lambda x: round(x.median(), 2),
                fill_value="",
            )
            .astype(str)
            .reset_index(drop=True)
        )

        # Create a pivot table for the weekly unique learners count
        weekly_unique_learners = (
            pd.pivot_table(
                final_overall_grad_jump_dt,
                columns="week_range",
                values="learner_id",
                aggfunc="nunique",
                fill_value="",
            )
            .astype(str)
            .reset_index(drop=True)
        )

        # Combine the weekly median time and unique learners count
        weekly_grd_jump_median_time = (
            weekly_median_time + " (" + weekly_unique_learners + ")"
        )
        weekly_grd_jump_median_time.reset_index(names=["metrics"], inplace=True)
        weekly_grd_jump_median_time["metrics"] = weekly_grd_jump_median_time[
            "metrics"
        ].astype("object")
        weekly_grd_jump_median_time.loc[0, "metrics"] = (
            "median time (in min) for a grade jump"
        )
    else:
        # If the DataFrame is empty, create a DataFrame with the required columns but no data
        weekly_grd_jump_median_time = pd.DataFrame(
            {"metrics": ["median time (in min) for a grade jump"]}
        )
        final_overall_grad_jump_dt = pd.DataFrame(
            columns=[
                "learner_id",
                "previous_qset_grade",
                "previous_grade_time",
                "min_timestamp",
                "week_range",
            ]
        )

    # Create a pivot table for the grade-wise weekly representation of median time-taken for grade jump
    grd_wise_weekly_median_time = pd.pivot_table(
        final_overall_grad_jump_dt,
        index="previous_qset_grade",
        columns="week_range",
        values=["previous_grade_time", "learner_id"],
        aggfunc={
            "previous_grade_time": lambda x: round(x.median(), 2),
            "learner_id": "nunique",
        },
    )

    if not grd_wise_weekly_median_time.empty:
        # Format the median time with the count of learners
        grd_wise_weekly_median_time["previous_grade_time"] = (
            grd_wise_weekly_median_time["previous_grade_time"].astype(str)
            + " ("
            + grd_wise_weekly_median_time["learner_id"].astype(str)
            + ")"
        )

        # Drop the learner_id column as it's now part of the string
        grd_wise_weekly_median_time.drop(columns=["learner_id"], inplace=True)

    grd_wise_weekly_median_time = grd_wise_weekly_median_time.droplevel(0, axis=1)
    # Reindex the DataFrame based on sorted qset-grades
    grd_wise_weekly_median_time = grd_wise_weekly_median_time.reindex(
        grades_ls, fill_value=pd.NA
    )
    grd_wise_weekly_median_time = grd_wise_weekly_median_time.reset_index().rename(
        columns={"grade": "sub_metrics"}
    )

    # Remove 'class-six' from the index as it's not required
    grd_wise_weekly_median_time = grd_wise_weekly_median_time[
        grd_wise_weekly_median_time["sub_metrics"] != "class-six"
    ]

    # Map grade jumps to their corresponding descriptions
    grade_jump_map = {
        "class-one": "class-one to class-two",
        "class-two": "class-two to class-three",
        "class-three": "class-three to class-four",
        "class-four": "class-four to class-five",
        "class-five": "class-five to class-six",
    }

    # Apply the grade jump map to the sub_metrics column
    grd_wise_weekly_median_time["sub_metrics"] = grd_wise_weekly_median_time[
        "sub_metrics"
    ].map(grade_jump_map)

    # Concatenate the weekly grade jump median time and the grade-wise weekly median time
    weekly_grade_jump_median_time = pd.concat(
        [weekly_grd_jump_median_time, grd_wise_weekly_median_time]
    )

    # Remove NaN values from the weekly_grade_jump_median_time DataFrame
    weekly_grade_jump_median_time.replace("nan (nan)", "", inplace=True)

    return overall_median_grade_jump_time, weekly_grade_jump_median_time


""" MEDIAN TIME TAKEN FOR OPERATOR JUMP (Addition to Subtraction) """
########################## - OVERALL
########################## - WEEK WISE

# Q: What is the definition of median time taken for operator jump?
# - Every qset solved by a learner is associated with an operation and a grade, called as "qset-grade".
# - When a learner moves from one operation to another, it is referred to as an "operation-jump".
# NOTE: Time spent by the learner on an operation includes both diagnostic and non-diagnostic qsets.
# 1. **Overall:**
#    - Median time taken by all learners to complete an operation jump across all operations.

# 2. **Overall Operation Wise (Addition to Subtraction):**
#    - Median time taken by all learners to jump from **Addition** to **Subtraction** operation.

# 3. **Weekly:**
#    - Median time taken by all learners to complete an operation jump within a week.

# 4. **Weekly Operation Wise (Addition to Subtraction):**
#    - Median time taken by all learners to jump from **Addition** to **Subtraction** within a week.


def get_median_time_for_operation_jump(
    all_learners_data, from_date, to_date, school, grade
):
    """
    This function calculates the median time taken for an operation jump across all operations and grades.
    It filters the data based on the provided grade and date range, calculates the median time for each operation jump,
    and returns the overall median time and the weekly median time for operation jumps.
    """
    operator_jump_df = get_operator_jump_data(all_learners_data)
    # operator_jump_df = all_operator_jump_data.copy()

    # Filter learners of selected school
    if school:
        operator_jump_df = operator_jump_df[operator_jump_df["school"] == school]
    # Filters data of selected grade
    if grade:
        operator_jump_df = operator_jump_df[operator_jump_df["grade"] == grade]

    if not operator_jump_df.empty:
        # Calculate time in seconds taken by a learner on an operation
        overall_operator_jump_df = (
            operator_jump_df.groupby(["learner_id", "operation", "operation_order"])
            .agg(
                total_time=("time_diff", "sum"),
                min_timestamp=("min_time", "min"),
                max_timestamp=("max_time", "max"),
            )
            .reset_index()
        )

        # Sort every learners data based on the operator
        overall_operator_jump_df.sort_values(
            by=["learner_id", "operation_order"],
            ascending=[True, True],
            inplace=True,
        )

        # Calculate the previous operation's time
        overall_operator_jump_df["previous_operation_time"] = (
            overall_operator_jump_df.groupby(["learner_id"])["total_time"].shift(1)
        )
        overall_operator_jump_df["previous_operation"] = (
            overall_operator_jump_df.groupby(["learner_id"])["operation"].shift(1)
        )

        overall_operator_jump_df = overall_operator_jump_df[
            overall_operator_jump_df["previous_operation_time"].notna()
        ]

        # Filters data within selected date range
        if from_date and to_date:
            overall_operator_jump_df = overall_operator_jump_df[
                (overall_operator_jump_df["min_timestamp"].dt.date >= from_date)
                & (overall_operator_jump_df["min_timestamp"].dt.date <= to_date)
            ]

        # Covert time in minutes
        overall_operator_jump_df["previous_operation_time"] = round(
            overall_operator_jump_df["previous_operation_time"] / 60, 2
        )
    else:
        overall_operator_jump_df = pd.DataFrame(
            columns=[
                "learner_id",
                "previous_operation",
                "previous_operation_time",
                "min_timestamp",
                "week_range",
            ]
        )

    # Create pivot table for operation-wise representation of median time-taken for operation jump
    operator_wise_overall_median_time = pd.pivot_table(
        overall_operator_jump_df,
        index="previous_operation",
        values=["previous_operation_time", "learner_id"],
        aggfunc={
            "previous_operation_time": lambda x: round(x.median(), 2),
            "learner_id": "nunique",
        },
    )

    if not operator_wise_overall_median_time.empty:
        operator_wise_overall_median_time["previous_operation_time"] = (
            operator_wise_overall_median_time["previous_operation_time"].astype(str)
            + " ("
            + operator_wise_overall_median_time["learner_id"].astype(str)
            + ")"
        )

        operator_wise_overall_median_time.drop(columns=["learner_id"], inplace=True)
    else:
        operator_wise_overall_median_time["previous_operation_time"] = np.nan

    # sort operations based on their priority order
    operators_ls = ["Addition", "Subtraction", "Multiplication", "Division"]
    operator_wise_overall_median_time = operator_wise_overall_median_time.reindex(
        operators_ls, fill_value=pd.NA
    )

    operator_wise_overall_median_time = operator_wise_overall_median_time[
        operator_wise_overall_median_time.index != "Division"
    ]

    # Create df for median time taken for operation-jump for a learner both on overall basis and operation-basis
    overall_median_operator_jump_time = pd.DataFrame(
        {
            "overall_count": [
                f"{round(overall_operator_jump_df['previous_operation_time'].median(), 2)} ({ unique_learners if (unique_learners:=overall_operator_jump_df['learner_id'].nunique()) else np.nan})"
            ]
            + operator_wise_overall_median_time["previous_operation_time"].to_list()
        }
    )

    overall_median_operator_jump_time.replace("nan (nan)", "", inplace=True)

    # Filters data for default if date range not selected
    if (not overall_operator_jump_df.empty) and not (from_date and to_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        from_date = most_recent_monday - timedelta(days=7 * 5)
        overall_operator_jump_df = overall_operator_jump_df[
            (overall_operator_jump_df["min_timestamp"].dt.date >= from_date.date())
        ]

    if not overall_operator_jump_df.empty:
        overall_operator_jump_df.loc[:, "week_range"] = overall_operator_jump_df[
            "min_timestamp"
        ].dt.date.apply(lambda x: calculate_range(x))

        # Create pivot table for weekly representation of median time-taken for operation jump
        weekly_median_time = (
            pd.pivot_table(
                overall_operator_jump_df,
                columns="week_range",
                values="previous_operation_time",
                aggfunc=lambda x: round(x.median(), 2),
                fill_value="",
            )
            .astype(str)
            .reset_index(drop=True)
        )

        weekly_unique_learners = (
            pd.pivot_table(
                overall_operator_jump_df,
                columns="week_range",
                values="learner_id",
                aggfunc="nunique",
                fill_value="",
            )
            .astype(str)
            .reset_index(drop=True)
        )

        weekly_operator_jump_median_time = (
            weekly_median_time + " (" + weekly_unique_learners + ")"
        )
        weekly_operator_jump_median_time.reset_index(names=["metrics"], inplace=True)

        weekly_operator_jump_median_time["metrics"] = weekly_operator_jump_median_time[
            "metrics"
        ].astype("object")
        weekly_operator_jump_median_time.loc[0, "metrics"] = (
            "median time (in min) for an operator jump"
        )
    else:
        weekly_operator_jump_median_time = pd.DataFrame(
            {"metrics": ["median time (in min) for an operator jump"]}
        )
        overall_operator_jump_df = pd.DataFrame(
            columns=[
                "learner_id",
                "previous_operation",
                "previous_operation_time",
                "min_timestamp",
                "week_range",
            ]
        )

    # Create pivot table for operation-wise weekly representation of median time-taken for operation jump
    operator_wise_weekly_median_time = pd.pivot_table(
        overall_operator_jump_df,
        index="previous_operation",
        columns="week_range",
        values=["previous_operation_time", "learner_id"],
        aggfunc={
            "previous_operation_time": lambda x: round(x.median(), 2),
            "learner_id": "nunique",
        },
    )

    if not operator_wise_weekly_median_time.empty:
        operator_wise_weekly_median_time["previous_operation_time"] = (
            operator_wise_weekly_median_time["previous_operation_time"].astype(str)
            + " ("
            + operator_wise_weekly_median_time["learner_id"].astype(str)
            + ")"
        )

        operator_wise_weekly_median_time.drop(columns=["learner_id"], inplace=True)
    operator_wise_weekly_median_time = operator_wise_weekly_median_time.droplevel(
        0, axis=1
    )

    # Reindex the df based on sorted operations
    operator_wise_weekly_median_time = operator_wise_weekly_median_time.reindex(
        operators_ls, fill_value=pd.NA
    )
    operator_wise_weekly_median_time = (
        operator_wise_weekly_median_time.reset_index().rename(
            columns={"previous_operation": "sub_metrics"}
        )
    )

    operator_wise_weekly_median_time.replace("nan (nan)", "", inplace=True)

    operator_wise_weekly_median_time = operator_wise_weekly_median_time[
        operator_wise_weekly_median_time["sub_metrics"] != "Division"
    ]

    operation_jump_map = {
        "Addition": "Addition to Subtraction",
        "Subtraction": "Subtraction to Multiplication",
        "Multiplication": "Multiplication to Division",
    }

    operator_wise_weekly_median_time["sub_metrics"] = operator_wise_weekly_median_time[
        "sub_metrics"
    ].map(operation_jump_map)

    # Concat to create operation-jump data
    weekly_operator_jump_median_time = pd.concat(
        [weekly_operator_jump_median_time, operator_wise_weekly_median_time]
    )
    return overall_median_operator_jump_time, weekly_operator_jump_median_time


def get_learners_metrics_data(
    all_learners_data_df,
    from_date: str,
    to_date: str,
    school: str,
    grade: str,
    operation: str,
):
    """
    Fetches learners' proficiency data and calculates various metrics such as unique learners, new learners added, sessions count, work done, and time taken for grade and operation jumps.

    Parameters:
    - from_date (str): The start date of the period for which metrics are to be calculated.
    - to_date (str): The end date of the period for which metrics are to be calculated.
    - grade (str): The grade level for which metrics are to be calculated.
    - operation (str): The operation type for which metrics are to be calculated.

    Returns:
    - final_table_df (DataFrame): A DataFrame containing the calculated metrics.
    """
    # Fetch learners proficiency data
    # all_learners_data_df = all_learners_data.copy()
    print("Shape of all_learners_data_df: ", all_learners_data_df.shape)

    # Determine the date range for calculation
    if not (from_date and to_date):
        # If no dates are provided, calculate the last 5 weeks from the current date
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        past_5_weeks_date = most_recent_monday - timedelta(days=7 * 5)
        from_date = past_5_weeks_date.date()
    else:
        # Convert provided dates to datetime format
        from_date = pd.to_datetime(from_date).date()
        to_date = pd.to_datetime(to_date).date()

    # Identify learners who were active before the start date
    previous_learners_list = all_learners_data_df[
        all_learners_data_df["updated_at"].dt.date < from_date
    ]["learner_id"].unique()

    # Convert 'updated_at' to 'updated_date' for easier date-based operations
    if not all_learners_data_df.empty:
        all_learners_data_df["updated_date"] = all_learners_data_df[
            "updated_at"
        ].dt.date

    """ UNIQUE LEARNERS LOGIC"""
    # Calculate unique learners, weekly unique learners count, and final unique learners DataFrame
    overall_unique_learners, weekly_uni_lrs_cnt_table, final_unique_learners_df = (
        get_unique_learners(
            all_learners_data_df, from_date, to_date, school, grade, operation
        )
    )

    """ NEW LEARNERS ADDED LOGIC """
    # Calculate new learners added
    weekly_new_learners_added = get_new_learners_added(
        final_unique_learners_df, previous_learners_list
    )

    """ SESSIONS COUNT LOGIC"""
    # Calculate overall sessions and weekly sessions count
    overall_sessions, weekly_sessions_cnt_table = get_sessions(
        all_learners_data_df, from_date, to_date, school, grade
    )

    """ WORK DONE LOGIC"""
    # Calculate overall work done, weekly work done, and work done DataFrame
    overall_work_done, weekly_work_done, work_done_df = get_work_done(
        all_learners_data_df, from_date, to_date, school, grade, operation
    )

    """ WORK DONE PER LEARNER LOGIC"""
    # Calculate average work done per learner and weekly work done per learner
    overall_work_done_avg, weekly_work_done_per_lr = get_avg_work_done_per_learner(
        work_done_df, overall_work_done, overall_unique_learners
    )

    """ MEDIAN WORK DONE PER LEARNER LOGIC"""
    # Calculate median work done per learner
    overall_median_work_done, weekly_median_work_done = (
        get_median_work_done_per_learner(work_done_df)
    )

    """ TOTAL TIME TAKEN LOGIC """
    # Calculate overall time taken, weekly total time, and total time DataFrame
    overall_time_taken, weekly_total_time, total_time_df = get_total_time_taken(
        all_learners_data_df, from_date, to_date, school, grade, operation
    )

    """ AVERAGE TIME PER LEARNER LOGIC"""
    # Calculate average time taken per learner and weekly time taken per learner
    overall_time_taken_avg, weekly_time_taken_per_lr = avg_time_taken_per_learner(
        total_time_df, overall_time_taken, overall_unique_learners
    )

    """MEDIAN ACCURACY OF LEARNERS LOGIC"""

    # Calculate median accuracy of learners
    overall_median_accuracy, weekly_median_accuracy = get_median_accuracy(
        all_learners_data_df, from_date, to_date, school, grade, operation
    )

    """ MEDIAN TIME TAKEN FOR GRADE JUMP """

    # Calculate median time taken for grade jump
    overall_median_grade_jump_time, weekly_grade_jump_median_time = (
        get_median_time_for_grade_jump(
            all_learners_data_df, from_date, to_date, school, grade, operation
        )
    )

    """ MEDIAN TIME TAKEN FOR OPERATOR JUMP"""
    # Calculate median time taken for operator jump
    overall_median_operator_jump_time, weekly_operator_jump_median_time = (
        get_median_time_for_operation_jump(
            all_learners_data_df, from_date, to_date, school, grade
        )
    )

    # Calculate final table data
    overall_count_df = pd.concat(
        [
            overall_unique_learners,
            pd.DataFrame([{"overall_count": None}]),
            overall_sessions,
            overall_time_taken,
            overall_time_taken_avg,
            overall_median_grade_jump_time,
            overall_median_operator_jump_time,
            overall_work_done,
            overall_work_done_avg,
            overall_median_work_done,
            overall_median_accuracy,
        ],
        ignore_index=True,
    )
    weekly_metrics_df = pd.concat(
        [
            weekly_uni_lrs_cnt_table,
            weekly_new_learners_added,
            weekly_sessions_cnt_table,
            weekly_total_time,
            weekly_time_taken_per_lr,
            weekly_grade_jump_median_time,
            weekly_operator_jump_median_time,
            weekly_work_done,
            weekly_work_done_per_lr,
            weekly_median_work_done,
            weekly_median_accuracy,
        ],
        ignore_index=True,
    ).rename_axis(None, axis=1)

    weekly_metrics_df["metrics"] = weekly_metrics_df["metrics"].str.title()
    final_table_df = pd.concat([overall_count_df, weekly_metrics_df], axis=1)

    return final_table_df


# Callback to update the master table based on selected filters
@callback(
    Output("dig-lpm-learner-perf-data-table", "data"),
    Output("dig-lpm-learner-perf-data-table", "columns"),
    Output("dig-lpm-learner-perf-data-table", "style_data_conditional"),
    Output("last-synced-on", "children"),
    Output("last-record-updated-at", "children"),
    Output("dig-lpm-dates-picker", "min_date_allowed"),
    Output("dig-lpm-dates-picker", "max_date_allowed"),
    Output("dig-lpm-schools-dropdown", "options"),
    Output("dig-lpm-grades-dropdown", "options"),
    Output("dig-ll-schools-dropdown", "options"),
    Output("dig-ll-grades-dropdown", "options"),
    Output("dig-ll-qset-purpose-dropdown", "options"),
    Output("dig-li-schools-dropdown", "options"),
    Output("dig-li-qset-grades-dropdown", "options"),
    Output("dig-li-qset-purpose-dropdown", "options"),
    Input("dig-lpm-dates-picker", "start_date"),
    Input("dig-lpm-dates-picker", "end_date"),
    Input("dig-lpm-schools-dropdown", "value"),
    Input("dig-lpm-grades-dropdown", "value"),
    Input("dig-lpm-operations-dropdown", "value"),
)
def update_table(
    start_date,
    end_date,
    selected_school,
    selected_grade,
    selected_operation,
):
    all_learners_data = get_data(ALL_LEARNER_DATA_KEY)

    print("What about here")

    # Filter the DataFrame based on the selected schools, school_grades and learner_id
    filtered_data = get_learners_metrics_data(
        all_learners_data,
        start_date,
        end_date,
        selected_school,
        selected_grade,
        selected_operation,
    )

    # Adjust start_date and end_date if not provided to default to the last 5 weeks
    if not (start_date and end_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        start_date = (most_recent_monday - timedelta(days=7 * 5)).date()
        end_date = current_date.date()
    else:
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

    # Generate week ranges based on the selected date range
    week_columns = generate_week_ranges(start_date, end_date)
    # Define non-week columns for the table
    non_week_columns = [
        {"name": col.replace("_", " ").title(), "id": col}
        for col in ["metrics", "sub_metrics", "overall_count"]
    ]
    # Combine non-week columns with week columns
    columns = non_week_columns + week_columns

    # Define style data conditional for the table
    style_data_conditional = [
        *(
            {
                "if": {
                    "row_index": index,
                },
                "backgroundColor": "#CDD6DF",
            }
            for index in [1, 2, 3, 4, 10, 11, 12, 13, 14, 16, 17, 18]
        ),
        *(
            {
                "if": {
                    "row_index": 8,  # Specific row index (e.g., the second row)
                    "column_id": col,
                    "filter_query": f"{{{col}}} < 30",  # Column filter
                },
                "backgroundColor": "lightcoral",
            }
            for col in ["overall_count"]
            + [week_dict.get("id") for week_dict in week_columns]
        ),
    ]

    last_synced_on_str = f"Last Synced On: {datetime.fromisoformat(last_synced_on).astimezone(IST).strftime('%d %b %Y %H:%M') if (last_synced_on := last_synced_time()) else '-'}"
    last_record_updated_at_str = f"Last Record Updated At: {get_min_max_timestamp('max').astimezone(IST).strftime('%d %b %Y %H:%M')}"
    min_date_allowed = get_min_max_timestamp("min").date()
    max_date_allowed = get_min_max_timestamp("max").date()
    school_options = [
        {"label": school, "value": school} for school in get_data(ALL_SCHOOLS_KEY)
    ]
    grades_options = [
        {"label": school_grade, "value": school_grade}
        for school_grade in get_data(ALL_GRADES_KEY)
        .sort_values(by="id")["grade"]
        .unique()
    ]
    qset_type_options = [
        {"label": qset_type, "value": qset_type}
        for qset_type in get_data(ALL_QSET_TYPES_KEY)
    ]

    return (
        filtered_data.to_dict("records"),
        columns,
        style_data_conditional,
        last_synced_on_str,
        last_record_updated_at_str,
        min_date_allowed,
        max_date_allowed,
        school_options,
        grades_options,
        school_options,
        grades_options,
        qset_type_options,
        school_options,
        grades_options,
        qset_type_options,
    )  # Return the filtered data


# Callback for button which clears the selected date
@callback(
    [
        Output("dig-lpm-dates-picker", "start_date", allow_duplicate=True),
        Output("dig-lpm-dates-picker", "end_date", allow_duplicate=True),
    ],
    Input("dig-lpm-clear-dates-button", "n_clicks"),
    prevent_initial_call="initial_duplicate",
)
def clear_date_picker(n_clicks):
    # If no clicks yet, don't reset the dates
    if n_clicks is None or n_clicks == 0:
        return no_update, no_update  # Do not change anything
    # Reset the date picker to have no dates selected
    return None, None


# Callback to update selected dates text
@callback(
    Output("dig-lpm-selected-date(s)", "children", allow_duplicate=True),
    [
        Input("dig-lpm-dates-picker", "start_date"),
        Input("dig-lpm-dates-picker", "end_date"),
    ],
    prevent_initial_call="initial_duplicate",
)
# for displaying the selected date range  from date picker in better format because the date picker format cant be changed
def update_selected_dates(start_date, end_date):
    if start_date and end_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        start_date_str = f"{start_date_obj.day} {start_date_obj.strftime('%b %Y')} ({start_date_obj.strftime('%a')})"
        end_date_str = f"{end_date_obj.day} {end_date_obj.strftime('%b %Y')} ({end_date_obj.strftime('%a')})"

        return f"Selected Dates: {start_date_str} to {end_date_str}"
    return "*Select a Date Range*"


# Callback to update the learners list table based on selected filters
# This table only gets visible if the unique learners count of an operation is selected from master table.
@callback(
    Output("dig-learners-list-data-table", "data"),
    Output("dig-ll-div", "hidden"),
    Output("dig-ll-selected-filters", "children"),
    Input("dig-lpm-learner-perf-data-table", "active_cell"),
    Input("dig-ll-qset-purpose-dropdown", "value"),
    Input("dig-ll-schools-dropdown", "value"),
    Input("dig-ll-grades-dropdown", "value"),
    Input("dig-ll-operations-dropdown", "value"),
    Input("dig-lpm-dates-picker", "start_date"),
    Input("dig-lpm-dates-picker", "end_date"),
    Input("dig-lpm-schools-dropdown", "value"),
    Input("dig-lpm-grades-dropdown", "value"),
    Input("dig-lpm-operations-dropdown", "value"),
)
def update_learners_list_table(
    active_cell,
    qset_types,
    selected_school,
    selected_grade,
    selected_operation,
    from_date,
    to_date,
    parent_school,
    parent_grade,
    parent_operation,
):
    all_learners_data = get_data(ALL_LEARNER_DATA_KEY)
    print("Updating learners list table")

    # Initialize the learners attempt table data and other variables
    learners_attempt_table_data = pd.DataFrame(columns=["learner_id", "grade"])
    hidden = True
    selected_filters = ""

    # Check if an active cell is present
    if active_cell:
        row = active_cell["row"]
        operation_map = {
            1: "Addition",
            2: "Subtraction",
            3: "Multiplication",
            4: "Division",
        }
        column = active_cell["column_id"]
        is_week_column = check_column_format(column)
        is_overall_column = column == "overall_count"

        # Check if the column is a week column or an overall column and if the operation is valid
        if (is_week_column or is_overall_column) and (
            operation := operation_map.get(row)
        ):
            hidden = False
            if selected_operation:
                operation = selected_operation

            # Filter the learners attempts data based on the operation
            learners_attempts_data = all_learners_data[
                all_learners_data["operation"] == operation
            ].copy()

            # If no operation is selected and a parent operation is present, filter the data based on the parent operation
            if not selected_operation and parent_operation:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["operation"] == parent_operation
                ]

            # If the column is a week column, split the column to get the from and to dates
            if is_week_column:
                from_date, to_date = column.split(",")
                from_date_dt = pd.to_datetime(from_date).date()
                to_date_dt = pd.to_datetime(to_date).date()
            else:
                from_date_dt = pd.to_datetime(from_date).date() if from_date else None
                to_date_dt = pd.to_datetime(to_date).date() if to_date else None

            # Apply date filters if the dates are present
            if from_date_dt:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["updated_at"].dt.date >= from_date_dt
                ]
            if to_date_dt:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["updated_at"].dt.date <= to_date_dt
                ]

            if selected_school:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["school"] == selected_school
                ]
            elif parent_school:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["school"] == parent_school
                ]

            # Apply grade filters if the grade is selected
            if selected_grade:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["grade"] == selected_grade
                ]
            elif parent_grade:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["grade"] == parent_grade
                ]

            # Apply qset type filters if the qset types are selected
            if qset_types:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["purpose"].isin(qset_types)
                ]

            # Generate the selected time frame and operation
            selected_time_frame = (
                f"Time Frame: {from_date} - {to_date if to_date else 'present'}"
                if from_date or to_date
                else f"Time Frame: Overall"
            )
            selected_operation = f"Operation: {operation}"
            selected_filters = (
                f"SELECTED METRICS :- {selected_operation}, {selected_time_frame}"
            )

            # If the learners attempts data is not empty, group the data and generate the attempt and accuracy data
            if not learners_attempts_data.empty:
                grouped_data = (
                    learners_attempts_data.groupby(
                        [
                            "learner_id",
                            "learner_username",
                            "grade",
                            "qset_grade",
                        ]
                    )
                    .agg(
                        questions_attempted=("attempts_count", "count"),
                        accuracy=(
                            "score",
                            lambda x: round((x.sum() / x.count()) * 100, 2),
                        ),
                    )
                    .reset_index()
                )

                attempt_data = grouped_data.pivot_table(
                    index="learner_id",
                    columns="qset_grade",
                    values="questions_attempted",
                )

                accuracy_data = grouped_data.pivot_table(
                    index="learner_id",
                    columns="qset_grade",
                    values="accuracy",
                ).rename(
                    columns={
                        "class-one": "class-one-avg",
                        "class-two": "class-two-avg",
                        "class-three": "class-three-avg",
                        "class-four": "class-four-avg",
                        "class-five": "class-five-avg",
                    }
                )

                # Concatenate the attempt and accuracy data
                learners_attempt_table_data = pd.concat(
                    [attempt_data, accuracy_data], axis=1
                ).reset_index()

                # Map the grade to the learner attempt table data
                grade_mapping = grouped_data.set_index("learner_id")["grade"].to_dict()
                learners_attempt_table_data["grade"] = learners_attempt_table_data[
                    "learner_id"
                ].map(grade_mapping)

                learner_name_mapping = learners_attempts_data.set_index("learner_id")[
                    "learner_name"
                ].to_dict()
                learners_attempt_table_data["learner_name"] = (
                    learners_attempt_table_data["learner_id"].map(learner_name_mapping)
                )

                learner_username_mapping = grouped_data.set_index("learner_id")[
                    "learner_username"
                ].to_dict()
                learners_attempt_table_data["learner_username"] = (
                    learners_attempt_table_data["learner_id"].map(
                        learner_username_mapping
                    )
                )

    # Return the learners attempt table data, hidden flag, and selected filters
    return learners_attempt_table_data.to_dict("records"), hidden, selected_filters


# Callback to update the learner info table based on selected filters
# This table only gets visible if the learner id of a learner is selected from learner list table.
@callback(
    Output("dig-learner-info-table", "data"),
    Output("dig-li-div", "hidden"),
    Output("dig-li-heading", "children"),
    Input("dig-learners-list-data-table", "active_cell"),
    Input("dig-learners-list-data-table", "data"),
    Input("dig-li-learner-dropdown", "value"),
    Input("dig-li-qset-grades-dropdown", "value"),
    Input("dig-li-operations-dropdown", "value"),
    Input("dig-li-qset-purpose-dropdown", "value"),
)
def update_learner_info_table(
    active_cell,
    data,
    learner_uni_name,
    qset_grade,
    operation,
    qset_types,
):
    all_learners_data = get_data(ALL_LEARNER_DATA_KEY)
    all_learners_data.drop(columns=["sequence"], inplace=True)

    # Initialize the learners performance data and other variables
    learners_perf_data = pd.DataFrame(
        columns=[
            "qset_name",
            "total_questions_attempted",
            "accuracy",
            "time_taken",
            "attempted_date",
        ]
    )
    hidden = True
    heading = ""

    # Check if an active cell is present and the data is not empty
    if active_cell and data and active_cell["row"] < len(data):
        column = active_cell["column_id"]

        # Check if the column is "learner_id"
        if column == "learner_id":
            hidden = False
            learner_id = data[active_cell["row"]]["learner_id"]
            if not learner_uni_name:
                selected_learner_data = all_learners_data[
                    all_learners_data["learner_id"] == learner_id
                ].copy()
            else:
                learner_username, learner_name = learner_uni_name.split("-")
                selected_learner_data = all_learners_data[
                    all_learners_data["learner_username"] == learner_username
                ].copy()
            heading = f"SELECTED LEARNER :- ID: {learner_id} , Grade: {data[active_cell['row']]['grade']}"

            # Apply filters based on the selected learner
            if qset_types:
                selected_learner_data = selected_learner_data[
                    selected_learner_data["purpose"].isin(qset_types)
                ]

            if qset_grade:
                selected_learner_data = selected_learner_data[
                    selected_learner_data["qset_grade"] == qset_grade
                ]

            if operation:
                selected_learner_data = selected_learner_data[
                    selected_learner_data["operation"] == operation
                ]

            # Check if the selected learner data is not empty
            if not selected_learner_data.empty:
                question_sequence_data = get_question_sequence_data()
                selected_learner_data = selected_learner_data.merge(
                    question_sequence_data,
                    how="left",
                    on=["question_id", "question_set_id"],
                )

                selected_learner_data.loc[:, "updated_date"] = selected_learner_data[
                    "updated_at"
                ].dt.date
                learners_qset_data = (
                    selected_learner_data.groupby(
                        ["learner_id", "question_set_id", "qset_name", "updated_date"]
                    )
                    .apply(
                        lambda gp: pd.Series(
                            {
                                "questions_attempted": gp["attempts_count"].count(),
                                "total_score": gp["score"].sum(),
                                "min_timestamp": gp["updated_at"].min(),
                                "max_timestamp": gp["updated_at"].max(),
                                "incorrect_attempts": ",".join(
                                    map(
                                        str,
                                        gp.loc[gp["score"] == 0, "sequence"]
                                        .sort_values()
                                        .unique(),
                                    )
                                ),
                            }
                        )
                    )
                    .reset_index()
                )
                learners_qset_data["time_taken"] = (
                    learners_qset_data["max_timestamp"]
                    - learners_qset_data["min_timestamp"]
                ).dt.total_seconds()
                learners_qset_data["time_taken"] = learners_qset_data[
                    "time_taken"
                ].clip(upper=2700)
                learners_perf_data = (
                    learners_qset_data.groupby(
                        ["learner_id", "question_set_id", "qset_name"]
                    )
                    .agg(
                        total_questions_attempted=("questions_attempted", "sum"),
                        total_score=("total_score", "sum"),
                        attempted_date=("updated_date", "max"),
                        time_taken=("time_taken", "sum"),
                        max_timestamp=("max_timestamp", "max"),
                        incorrect_attempts=(
                            "incorrect_attempts",
                            lambda x: ",".join(s.strip() for s in x if s.strip()),
                        ),
                    )
                    .reset_index()
                )
                learners_perf_data.loc[:, "accuracy"] = round(
                    (
                        learners_perf_data["total_score"]
                        / learners_perf_data["total_questions_attempted"]
                    )
                    * 100,
                    2,
                )
                learners_perf_data.loc[:, "time_taken"] = round(
                    learners_perf_data["time_taken"] / 60, 2
                )
                learners_perf_data.sort_values(
                    by="max_timestamp", ascending=True, inplace=True
                )
                learners_perf_data.loc[
                    learners_perf_data.index[-1] :, "attempted_date"
                ] = "in progress"

    return learners_perf_data.to_dict("records"), hidden, heading


###################################  Digital Master Dashboard Layout ###################################

layout = html.Div(
    [
        html.H1("Master Dashboard"),
        html.Div(
            [
                html.H5(
                    id="last-synced-on",
                    style={"margin": "0px"},
                ),
                html.H5(
                    id="last-record-updated-at",
                    style={"margin": "0px"},
                ),
            ],
            style={
                "display": "flex",
                "flexDirection": "column",  # Stack vertically
                "alignItems": "flex-end",  # Align text to the right
                "flexWrap": "wrap",
            },
        ),
        html.Div(
            [
                # Clear Dates Button
                html.Button(
                    "Clear Dates",
                    id="dig-lpm-clear-dates-button",
                    n_clicks=0,
                    style={
                        "marginRight": "10px",
                        "height": "55px",
                        "width": "55px",
                        "borderRadius": "35%",
                        "background": "linear-gradient(to right, #EDEDED, #DCDCDC)",
                        "border": "2px solid black",
                        "cursor": "pointer",
                    },
                ),
                dcc.DatePickerRange(id="dig-lpm-dates-picker"),
                # Dropdown for selecting school
                dcc.Dropdown(
                    id="dig-lpm-schools-dropdown",
                    placeholder="Select School",
                    style={"width": "300px", "margin": "10px"},
                ),
                # Dropdown for selecting grade
                dcc.Dropdown(
                    id="dig-lpm-grades-dropdown",
                    placeholder="Select Grade",
                    style={"width": "300px", "margin": "10px"},
                ),
                # Dropdown for selecting operations
                dcc.Dropdown(
                    id="dig-lpm-operations-dropdown",
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
            ],
            style={
                "display": "flex",
                "justifyContent": "flex-start",  # Center items horizontally
                "alignItems": "center",  # Align items vertically
                "flexWrap": "wrap",  # Allow items to wrap to the next line if needed
                "marginBottom": "10px",
            },
        ),
        html.Div(
            id="dig-lpm-selected-date(s)",
            style={"margin": "10px 0px", "fontSize": "16px"},
        ),
        # DataTable to display the filtered data
        dcc.Loading(
            id="dig-lpm-loading-table",
            type="circle",
            children=[
                dash_table.DataTable(
                    id="dig-lpm-learner-perf-data-table",
                    style_table={
                        "overflowX": "auto",
                        "maxWidth": "100%",
                        "marginBottom": "50px",
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
                        # "zIndex": "1",
                        "text-align": "center",
                    },  # Sticky header
                    data=[],
                    style_cell={
                        "textAlign": "center",
                        "minWidth": "150px",
                        "maxWidth": "300px",
                        "overflow": "hidden",
                        "textOverflow": "ellipsis",
                    },
                )
            ],
        ),
        # DataTable to display the filtered data
        html.Div(
            id="dig-ll-div",
            children=[
                html.H4(id="dig-ll-selected-filters", style={"margin": "10px 10px"}),
                html.Div(
                    [
                        # Dropdown for selecting school
                        dcc.Dropdown(
                            id="dig-ll-schools-dropdown",
                            placeholder="Select School",
                            style={"width": "300px", "margin": "10px"},
                        ),
                        # Dropdown for selecting grade
                        dcc.Dropdown(
                            id="dig-ll-grades-dropdown",
                            placeholder="Select Grade",
                            style={"width": "300px", "margin": "10px"},
                        ),
                        # Dropdown for selecting operations
                        dcc.Dropdown(
                            id="dig-ll-operations-dropdown",
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
                        dcc.Dropdown(
                            id="dig-ll-qset-purpose-dropdown",
                            multi=True,
                            placeholder="Select Question Set Type",
                            style={"width": "300px", "margin": "10px"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "flexWrap": "wrap",
                        "margin": "0px 10px",
                        "alignItems": "center",
                    },
                ),
                dcc.Loading(
                    id="dig-ll-loading-table",
                    type="circle",
                    children=[
                        dash_table.DataTable(
                            id="dig-learners-list-data-table",
                            columns=[
                                {"name": ["", "Learner ID"], "id": "learner_id"},
                                {"name": ["", "Name"], "id": "learner_name"},
                                {"name": ["", "Username"], "id": "learner_username"},
                                {"name": ["", "class"], "id": "grade"},
                                {
                                    "name": ["Attempt Data", "class-one"],
                                    "id": "class-one",
                                },
                                {
                                    "name": ["Attempt Data", "class-two"],
                                    "id": "class-two",
                                },
                                {
                                    "name": ["Attempt Data", "class-three"],
                                    "id": "class-three",
                                },
                                {
                                    "name": ["Attempt Data", "class-four"],
                                    "id": "class-four",
                                },
                                {
                                    "name": ["Attempt Data", "class-five"],
                                    "id": "class-five",
                                },
                                {
                                    "name": ["Accuracy (%)", "class-one"],
                                    "id": "class-one-avg",
                                },
                                {
                                    "name": ["Accuracy (%)", "class-two"],
                                    "id": "class-two-avg",
                                },
                                {
                                    "name": ["Accuracy (%)", "class-three"],
                                    "id": "class-three-avg",
                                },
                                {
                                    "name": ["Accuracy (%)", "class-four"],
                                    "id": "class-four-avg",
                                },
                                {
                                    "name": ["Accuracy (%)", "class-five"],
                                    "id": "class-five-avg",
                                },
                            ],
                            merge_duplicate_headers=True,
                            style_table={
                                "overflowX": "auto",
                                "overflowY": "auto",
                                "maxWidth": "100%",
                                "marginBottom": "30px",
                                "maxHeight": "500px",
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
                                # "zIndex": "1",
                                "text-align": "center",
                            },  # Sticky header
                            page_size=20,  # Set the number of rows per page
                            data=[],
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
                                            "column_id": id,
                                        },
                                        "backgroundColor": "#CDD6DF",
                                    }
                                    for id in [
                                        "class-one",
                                        "class-two",
                                        "class-three",
                                        "class-four",
                                        "class-five",
                                    ]
                                ),
                                *(
                                    {
                                        "if": {
                                            "column_id": id,
                                        },
                                        "backgroundColor": "#BEC7D0",
                                    }
                                    for id in [
                                        "class-one-avg",
                                        "class-two-avg",
                                        "class-three-avg",
                                        "class-four-avg",
                                        "class-five-avg",
                                    ]
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="dig-li-div",
                    children=[
                        html.H4(id="dig-li-heading", style={"margin": "10px 10px"}),
                        html.Div(
                            [
                                # Dropdown for selecting school
                                dcc.Dropdown(
                                    id="dig-li-schools-dropdown",
                                    placeholder="Select School",
                                    style={"width": "300px", "margin": "10px"},
                                ),
                                # Dropdown for selecting learner_id
                                dcc.Dropdown(
                                    id="dig-li-learner-dropdown",
                                    placeholder="Select Learner",
                                    style={"width": "300px", "margin": "10px"},
                                ),
                                # Dropdown for selecting grade
                                dcc.Dropdown(
                                    id="dig-li-qset-grades-dropdown",
                                    placeholder="Select Qset Grade",
                                    style={"width": "300px", "margin": "10px"},
                                ),
                                # Dropdown for selecting operations
                                dcc.Dropdown(
                                    id="dig-li-operations-dropdown",
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
                                dcc.Dropdown(
                                    id="dig-li-qset-purpose-dropdown",
                                    multi=True,
                                    placeholder="Select Question Set Type",
                                    style={"width": "300px", "margin": "10px"},
                                ),
                            ],
                            style={
                                "display": "flex",
                                "flexWrap": "wrap",
                                "margin": "0px 10px",
                                "alignItems": "center",
                            },
                        ),
                        dcc.Loading(
                            id="dig-li-loading-table",
                            type="circle",
                            children=[
                                dash_table.DataTable(
                                    id="dig-learner-info-table",
                                    columns=[
                                        {"name": "Set Name", "id": "qset_name"},
                                        {
                                            "name": "Attempt Data",
                                            "id": "total_questions_attempted",
                                        },
                                        {"name": "Accuracy (%)", "id": "accuracy"},
                                        {
                                            "name": "Date Attempted",
                                            "id": "attempted_date",
                                        },
                                        {"name": "Time Taken", "id": "time_taken"},
                                        {
                                            "name": "Sequence Of Wrong Questions",
                                            "id": "incorrect_attempts",
                                        },
                                    ],
                                    style_table={
                                        "overflowX": "auto",
                                        "overflowY": "auto",
                                        "maxWidth": "100%",
                                        "marginBottom": "50px",
                                        "maxHeight": "500px",
                                    },
                                    style_data={
                                        "whiteSpace": "pre-wrap",  # Preserve newlines
                                        "height": "auto",  # Adjust row height
                                        "textAlign": "center",
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
                                        # "zIndex": "1",
                                        "text-align": "center",
                                    },  # Sticky header
                                    page_size=20,  # Set the number of rows per page
                                    data=[],
                                    style_cell={
                                        "textAlign": "center",
                                        "minWidth": "150px",
                                        "maxWidth": "200px",
                                        "overflowX": "auto",
                                        # "textOverflow": "ellipsis",
                                    },
                                )
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ]
)
