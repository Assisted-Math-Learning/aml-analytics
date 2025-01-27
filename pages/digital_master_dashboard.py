import config
import dash
import numpy as np
import pandas as pd
import re
from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Register the page
# app = Dash(__name__)
# server = app.server
dash.register_page(__name__)


# Create the connection string for the database
connection_string = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Minimum - Maximum Date
min_max_date = pd.read_sql(
    "SELECT MIN(updated_at), MAX(updated_at) FROM learner_proficiency_question_level_data",
    engine,
)

# Grades List
grades = pd.read_sql("SELECT id, cm.name->>'en' AS grade FROM class_master cm", engine)

# Qset Types
qset_types = pd.read_sql("SELECT DISTINCT(purpose) FROM question_set", engine)

# Qset Types
question_sequence_data = pd.read_sql(
    "SELECT question_id, question_set_id, sequence FROM question_set_question_mapping",
    engine,
)

all_learners_data = pd.read_sql(
    f"""
    SELECT
    lpd.learner_id,
    lpd.question_id,
    lpd.question_set_id,
    qs.title->>'en' AS qset_name,
    lpd.updated_at,
    lpd.attempts_count,
    lpd.score,
    lpd.taxonomy->'class'->'name'->'en' AS qset_grade,
    lpd.taxonomy->'l1_skill'->'name'->'en' AS operation,
    cm.name->>'en' AS grade,
    qs.purpose
    FROM learner_proficiency_question_level_data lpd
    LEFT JOIN question_set qs ON lpd.question_set_id = qs.identifier
    LEFT JOIN learner lr ON lpd.learner_id = lr.identifier
    LEFT JOIN class_master cm ON lr.taxonomy->'class'->>'identifier' = cm.identifier
    """,
    engine,
)
all_learners_data["updated_at"] = pd.to_datetime(all_learners_data["updated_at"])


def get_overall_unique_learners(
    from_date: str, to_date: str, grade: str, operation: str
):
    uni_learners_df = all_learners_data.copy()
    if from_date and to_date:
        uni_learners_df = uni_learners_df[
            (uni_learners_df["updated_at"].dt.date >= from_date)
            & (uni_learners_df["updated_at"].dt.date <= to_date)
        ]

    # Apply grade filter if 'grade' is provided
    if grade:
        uni_learners_df = uni_learners_df[uni_learners_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        uni_learners_df = uni_learners_df[uni_learners_df["operation"] == operation]

    # Count distinct learners
    overall_count = uni_learners_df["learner_id"].nunique()
    overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    op_wise_uni_learners_df = (
        uni_learners_df.groupby(["operation"])["learner_id"]
        .nunique()
        .reset_index(name="overall_count")
    )
    operators_ls = ["Addition", "Subtraction", "Multiplication", "Division"]
    op_wise_uni_learners_df = op_wise_uni_learners_df.set_index("operation").reindex(
        operators_ls, fill_value=pd.NA
    )
    op_wise_uni_learners_df = op_wise_uni_learners_df.reset_index(drop=True)
    overall_uni_learners_df = pd.concat(
        [overall_count_df, op_wise_uni_learners_df]
    ).reset_index(drop=True)

    return overall_uni_learners_df


def get_unique_learners(all_learners_data, from_date, to_date, grade, operation):
    # Fetch overall unique learners count
    overall_unique_learners = get_overall_unique_learners(
        from_date, to_date, grade, operation
    )

    # Create copy of all learners data
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
        )

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


def get_new_learners_added(final_unique_learners_df, previous_learners_list):
    # Create copy of filtered unique learners data
    learners_added_df = final_unique_learners_df.copy()

    # Sort the data based on date
    learners_added_df = learners_added_df.sort_values("updated_date")

    # Initialize a set to track learner_ids seen in previous weeks
    previous_learners = set(previous_learners_list)

    # Iterate over rows and calculate count of learners for the current week who haven't appeared before
    def count_new_learners_for_week(current_week, current_learners):
        # Learners in current week who have not appeared in previous weeks
        new_learners = [
            learner for learner in current_learners if learner not in previous_learners
        ]
        # Update the previous_learners set with the current week's learners
        previous_learners.update(current_learners)
        return len(new_learners)

    # Apply the functtime_taken_per_learner_dfion to each week in the DataFrame
    learners_added_df["new learners added"] = learners_added_df.groupby("week_range")[
        "active_learners"
    ].transform(lambda x: count_new_learners_for_week(x.name, x.tolist()))

    # Apply Pivot Table to get new learners added per week
    weekly_new_learners_added = learners_added_df.pivot_table(
        values="new learners added", columns="week_range"
    ).reset_index(names=["metrics"])

    return weekly_new_learners_added


def get_overall_sessions(from_date: str, to_date: str, grade: str):
    uni_sessions_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        uni_sessions_df = uni_sessions_df[
            (uni_sessions_df["updated_at"].dt.date >= from_date)
            & (uni_sessions_df["updated_at"].dt.date <= to_date)
        ]

    # Apply grade filter if 'grade' is provided
    if grade:
        uni_sessions_df = uni_sessions_df[uni_sessions_df["grade"] == grade]

    # Check if DataFrame is empty after filtering
    if uni_sessions_df.empty:
        overall_count_df = pd.DataFrame([{"overall_count": 0}])
    else:
        # Group by date and grade, and count distinct learners (learner_id)
        session_groups = (
            uni_sessions_df.groupby([uni_sessions_df["updated_at"].dt.date, "grade"])
            .agg(total_learners=("learner_id", "nunique"))
            .reset_index()
        )

        # Filter out groups where the number of distinct learners is less than 3
        session_groups = session_groups[session_groups["total_learners"] >= 3]

        # Check if DataFrame is empty after filtering
        if session_groups.empty:
            overall_count_df = pd.DataFrame([{"overall_count": 0}])
        else:
            # Count the number of remaining sessions
            overall_count = session_groups.shape[0]

            # Create the final result as a DataFrame
            overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    return overall_count_df


def get_sessions(all_learners_data, from_date, to_date, grade):
    # Fetch overall sessions count
    overall_sessions = get_overall_sessions(from_date, to_date, grade)

    # Create copy of all learners data
    weekly_sessions_data = all_learners_data.copy()

    # Filter learners records after from_date
    if from_date:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["updated_at"].dt.date >= from_date
        ]

    # Filter learners records till to_date
    if to_date:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["updated_at"].dt.date <= to_date
        ]

    # Filters learners of selected grade
    if grade:
        weekly_sessions_data = weekly_sessions_data[
            weekly_sessions_data["grade"] == grade
        ]

    if not weekly_sessions_data.empty:
        # Get unique learners of respective grades on every date
        weekly_sessions_data = weekly_sessions_data.drop_duplicates(
            subset=["grade", "updated_date", "learner_id"]
        )

        # Map week range to every entry based on date
        weekly_sessions_data.loc[:, "week_range"] = weekly_sessions_data[
            "updated_date"
        ].apply(lambda x: calculate_range(x))

        # Count no. of unique learners of respective grades on every date
        grouped_session_data = (
            weekly_sessions_data.groupby(["grade", "week_range", "updated_date"])[
                "learner_id"
            ]
            .count()
            .reset_index()
        )

        # If the no. of unique learners for any grade on any date is equal or greater than 3
        # then mark that as a session = 1 else 0
        grouped_session_data["sessions"] = (
            grouped_session_data["learner_id"] >= 3
        ).astype("int")

        # Create pivot table for weekly representation of sessions count
        weekly_sessions_cnt_table = pd.pivot_table(
            grouped_session_data, values="sessions", columns="week_range", aggfunc="sum"
        ).reset_index(names=["metrics"])
    else:
        weekly_sessions_cnt_table = pd.DataFrame({"metrics": ["sessions"]})

    return overall_sessions, weekly_sessions_cnt_table


def get_overall_work_done(from_date: str, to_date: str, grade: str, operation: str):
    work_done_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        work_done_df = work_done_df[
            (work_done_df["updated_at"].dt.date >= from_date)
            & (work_done_df["updated_at"].dt.date <= to_date)
        ]

    # Apply grade filter if 'grade' is provided
    if grade:
        work_done_df = work_done_df[work_done_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        work_done_df = work_done_df[work_done_df["operation"] == operation]

    # Check if DataFrame is empty after date filter
    if work_done_df.empty:
        overall_count_df = pd.DataFrame([{"overall_count": 0}])
    else:
        # Count distinct learners
        overall_count = work_done_df.shape[0]

        # Create the final result as a DataFrame
        overall_count_df = pd.DataFrame([{"overall_count": overall_count}])

    return overall_count_df


def get_work_done(
    all_learners_data, from_date: str, to_date: str, grade: str, operation: str
):
    # Fetch overall work done
    overall_work_done = get_overall_work_done(from_date, to_date, grade, operation)

    # Create copy of all learners data
    work_done_df = all_learners_data.copy()

    # Filter learners records after from_date
    if from_date:
        work_done_df = work_done_df[work_done_df["updated_at"].dt.date >= from_date]

    # Filter learners records till to_date
    if to_date:
        work_done_df = work_done_df[work_done_df["updated_at"].dt.date <= to_date]

    # Filters data of selected grade
    if grade:
        work_done_df = work_done_df[work_done_df["grade"] == grade]
    # Filter data of selected operation
    if operation:
        work_done_df = work_done_df[work_done_df["operation"] == operation]

    if not work_done_df.empty:
        # Map week range to every entry based on date
        work_done_df.loc[:, "week_range"] = work_done_df["updated_date"].apply(
            lambda x: calculate_range(x)
        )

        # Rename 'attempts_count' to work done
        work_done_df.rename(columns={"attempts_count": "work done"}, inplace=True)

        # Create pivot table for weekly representation of work done
        weekly_work_done = pd.pivot_table(
            work_done_df, values="work done", columns="week_range", aggfunc="count"
        ).reset_index(names=["metrics"])
    else:
        weekly_work_done = pd.DataFrame({"metrics": ["work done"]})
        work_done_df = pd.DataFrame(
            columns=[
                "work done",
                "learner_id",
                "week_range",
            ]
        )

    return overall_work_done, weekly_work_done, work_done_df


def get_avg_work_done_per_learner(
    work_done_df, overall_work_done, overall_unique_learners
):
    # Calculate overall work done per learner
    overall_work_done_per_lr = (
        overall_work_done.loc[0, "overall_count"]
        // overall_unique_learners.loc[0, "overall_count"]
    )
    overall_work_done_avg = pd.DataFrame([{"overall_count": overall_work_done_per_lr}])

    # Create copy of filtered work done data
    work_done_per_learner_df = work_done_df.copy()

    # Count no. of unique learners who worked in respective weeks
    work_done_per_learner_df["unique_learners"] = work_done_per_learner_df[
        "week_range"
    ].map(work_done_per_learner_df.groupby("week_range")["learner_id"].nunique())

    if not work_done_per_learner_df.empty:
        # Create pivot table for weekly representation of work done per learner in that week
        weekly_work_done_per_lr = pd.pivot_table(
            work_done_per_learner_df,
            values=["work done", "unique_learners"],
            index="week_range",  # Use 'week_range' as index if it makes sense for your use case
            aggfunc={"work done": "count", "unique_learners": "first"},
        )
        weekly_work_done_per_lr["work done per learner"] = (
            weekly_work_done_per_lr["work done"]
            // weekly_work_done_per_lr["unique_learners"]
        )
        weekly_work_done_per_lr = weekly_work_done_per_lr[["work done per learner"]]
        weekly_work_done_per_lr = weekly_work_done_per_lr.transpose().reset_index(
            names=["metrics"]
        )
    else:
        weekly_work_done_per_lr = pd.DataFrame({"metrics": ["work done per learner"]})
    return overall_work_done_avg, weekly_work_done_per_lr


def get_median_work_done_per_learner(work_done_df):
    # Create copy of filtered work done data
    work_done_per_learner_df = work_done_df.copy()

    # Calculate overall median work done per learner
    overall_median_work_done_per_lr = (
        work_done_per_learner_df.groupby("learner_id")
        .agg(work_done=("work done", "count"))
        .reset_index()
    )
    overall_median_work_done = pd.DataFrame(
        [{"overall_count": overall_median_work_done_per_lr["work_done"].median()}]
    )

    if not work_done_per_learner_df.empty:
        # Create work done data for every learner in respective week ranges
        weekly_work_done_by_learners = (
            work_done_df.groupby(["learner_id", "week_range"])
            .agg(work_done=("work done", "count"))
            .reset_index()
        )

        # Create pivot table for weekly representation of median work done in that week
        weekly_median_work_done = pd.pivot_table(
            weekly_work_done_by_learners,
            values="work_done",
            index="week_range",  # Use 'week_range' as index if it makes sense for your use case
            aggfunc="count",
        )
        weekly_median_work_done = weekly_median_work_done.transpose().reset_index(
            names=["metrics"]
        )
        weekly_median_work_done.loc[0, "metrics"] = "Median Work Done Per Learner"
    else:
        weekly_median_work_done = pd.DataFrame(
            {"metrics": ["Median Work Done Per Learner"]}
        )
    return overall_median_work_done, weekly_median_work_done


def get_overall_time_taken(from_date: str, to_date: str, grade: str, operation: str):
    time_taken_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        time_taken_df = time_taken_df[
            (time_taken_df["updated_at"].dt.date >= from_date)
            & (time_taken_df["updated_at"].dt.date <= to_date)
        ]

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

        # Sum up the time difference and convert to minutes
        total_time_taken = round(time_taken_df_grouped["time_diff"].sum() / 60, 2)

    return pd.DataFrame([{"overall_count": total_time_taken}])


def get_total_time_taken(all_learners_data, from_date, to_date, grade, operation):
    # Fetch overall time taken
    overall_time_taken = get_overall_time_taken(from_date, to_date, grade, operation)

    # Create copy of all learners data
    total_time_df = all_learners_data.copy()

    # Filter learners records after from_date
    if from_date:
        total_time_df = total_time_df[total_time_df["updated_at"].dt.date >= from_date]

    # Filter learners records till to_date
    if to_date:
        total_time_df = total_time_df[total_time_df["updated_at"].dt.date <= to_date]

    # Filters data of selected grade
    if grade:
        total_time_df = total_time_df[total_time_df["grade"] == grade]
    # Filter data of selected operation
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

        # Set maximum value of 'time_diff' to 45 minutes = 2700
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


def avg_time_taken_per_learner(
    total_time_df, overall_time_taken, overall_unique_learners
):
    # Calculate overall time taken per learner
    overall_time_taken_per_lr = round(
        overall_time_taken.loc[0, "overall_count"]
        / overall_unique_learners.loc[0, "overall_count"],
        2,
    )
    overall_time_taken_avg = pd.DataFrame(
        [{"overall_count": overall_time_taken_per_lr}]
    )

    # Create copy of filtered total time taken data
    time_taken_per_learner_df = total_time_df.copy()

    # Count no. of unique learners who worked in respective weeks
    time_taken_per_learner_df["unique_learners"] = time_taken_per_learner_df[
        "week_range"
    ].map(time_taken_per_learner_df.groupby("week_range")["learner_id"].nunique())

    if not time_taken_per_learner_df.empty:
        # Create pivot table for weekly representation of time taken per learner in that week
        weekly_time_taken_per_lr = pd.pivot_table(
            time_taken_per_learner_df,
            values=["time_diff", "unique_learners"],
            index="week_range",  # Use 'week_range' as index if it makes sense for your use case
            aggfunc={
                "time_diff": lambda x: round(x.sum() / 60, 2),
                "unique_learners": "first",
            },
        )
        weekly_time_taken_per_lr["average time spent per learner (in min)"] = round(
            weekly_time_taken_per_lr["time_diff"]
            / weekly_time_taken_per_lr["unique_learners"],
            2,
        )
        weekly_time_taken_per_lr = weekly_time_taken_per_lr[
            ["average time spent per learner (in min)"]
        ]
        weekly_time_taken_per_lr = weekly_time_taken_per_lr.transpose().reset_index(
            names=["metrics"]
        )
    else:
        weekly_time_taken_per_lr = pd.DataFrame(
            {"metrics": ["average time spent per learner (in min)"]}
        )

    return overall_time_taken_avg, weekly_time_taken_per_lr


def get_overall_median_accuracy(
    from_date: str, to_date: str, grade: str, operation: str
):
    accuracy_df = all_learners_data.copy()

    # Apply date filter if 'from_date' and 'to_date' are provided
    if from_date and to_date:
        accuracy_df = accuracy_df[
            (accuracy_df["updated_at"].dt.date >= from_date)
            & (accuracy_df["updated_at"].dt.date <= to_date)
        ]

    # Apply grade filter if 'grade' is provided
    if grade:
        accuracy_df = accuracy_df[accuracy_df["grade"] == grade]

    # Apply operation filter if 'operation' is provided
    if operation:
        accuracy_df = accuracy_df[accuracy_df["operation"] == operation]

    # Check if DataFrame is empty after all filters
    if accuracy_df.empty:
        median_accuracy_df = pd.DataFrame(columns=["learner_id", "accuracy"])
    else:
        # Calculate accuracy per learner (SUM(score) / COUNT(score)) * 100
        accuracy_df_grouped = (
            accuracy_df.groupby("learner_id")
            .agg(accuracy=("score", lambda x: round((x.sum() / x.count()) * 100, 2)))
            .reset_index()
        )

        # Set result in 'median_accuracy_df'
        median_accuracy_df = accuracy_df_grouped[["learner_id", "accuracy"]]

    return pd.DataFrame(
        [{"overall_count": round(median_accuracy_df["accuracy"].median(), 2)}]
    )


def get_median_accuracy(all_learners_data, from_date, to_date, grade, operation):
    # Fetch overall median accuracy of learners
    overall_median_accuracy = get_overall_median_accuracy(
        from_date, to_date, grade, operation
    )

    # Create copy of all learners data
    median_accuracy_df = all_learners_data.copy()

    # Filter learners records after from_date
    if from_date:
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["updated_at"].dt.date >= from_date
        ]

    # Filter learners records till to_date
    if to_date:
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["updated_at"].dt.date <= to_date
        ]

    # Filters data of selected grade
    if grade:
        median_accuracy_df = median_accuracy_df[median_accuracy_df["grade"] == grade]
    # Filter data of selected operation
    if operation:
        median_accuracy_df = median_accuracy_df[
            median_accuracy_df["operation"] == operation
        ]

    if not median_accuracy_df.empty:
        # Map week range to every entry based on date
        median_accuracy_df.loc[:, "week_range"] = median_accuracy_df[
            "updated_date"
        ].apply(lambda x: calculate_range(x))

        # Calculate accuracy of every learner in respective weeks
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

        # Create pivot table for weekly representation of median accuracy per learner
        weekly_median_accuracy = pd.pivot_table(
            weekly_accuracy_per_learner,
            values="accuracy",
            columns="week_range",
            aggfunc=lambda x: round(x.median(), 2),
        ).reset_index(names=["metrics"])
        weekly_median_accuracy.loc[0, "metrics"] = "Median Accuracy Of Learners"
    else:
        weekly_median_accuracy = pd.DataFrame(
            {"metrics": ["Median Accuracy Of Learners"]}
        )

    return overall_median_accuracy, weekly_median_accuracy


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

grades_priority = grades.set_index("grade").to_dict().get("id")


# Define the logic for applying diff based on the sum of 'time_diff_clipped'
def apply_diff_or_copy(group):
    if group["time_diff_clipped"].sum() > 2700:
        # Apply diff only if sum of 'time_diff_clipped' > threshold
        group["time_diff_clipped"] = group["time_diff_clipped"].diff(1).abs()
    else:
        # If sum is <= threshold, keep 'time_diff_clipped' values
        group["time_diff_clipped"] = group["time_diff_clipped"]
    return group


def get_grade_jump_data():
    grade_jump_data = all_learners_data.copy()

    # Filtering the data as per the purpose column
    grade_jump_data = grade_jump_data[grade_jump_data["purpose"] != "Main Diagnostic"]

    # If the filtered data is empty, handle accordingly
    if not grade_jump_data.empty:
        grade_jump_data.loc[:, "date"] = grade_jump_data["updated_at"].dt.date

        # Aggregating data
        grade_jump_data = (
            grade_jump_data.groupby(
                ["learner_id", "grade", "operation", "qset_grade", "date"]
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
        grade_jump_data.loc[:, "grade_order"] = grade_jump_data["qset_grade"].map(
            grades_priority
        )
    return grade_jump_data


all_grade_jump_data = get_grade_jump_data()


def get_median_time_for_grade_jump(from_date, to_date, grade, operation):
    # Create copy of grade jump data
    grade_jump_df = all_grade_jump_data.copy()
    # Filters data of selected grade
    if grade:
        grade_jump_df = grade_jump_df[grade_jump_df["grade"] == grade]

    # Filter data of selected operation
    if operation:
        grade_jump_df = grade_jump_df[grade_jump_df["operation"] == operation]

    if not grade_jump_df.empty:
        # Calculate time in seconds taken by a learner on qset-grade of an operation
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

        # Sort every learners data based on the operator and the qset-grade
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
        ]

        # Filters data within selected date range
        if from_date and to_date:
            final_overall_grad_jump_dt = final_overall_grad_jump_dt[
                (final_overall_grad_jump_dt["min_timestamp"].dt.date >= from_date)
                & (final_overall_grad_jump_dt["min_timestamp"].dt.date >= to_date)
            ]

        # Covert time in minutes
        final_overall_grad_jump_dt["previous_grade_time"] = round(
            final_overall_grad_jump_dt["previous_grade_time"] / 60, 2
        )
    else:
        final_overall_grad_jump_dt = pd.DataFrame(
            columns=[
                "learner_id",
                "previous_qset_grade",
                "previous_grade_time",
                "min_timestamp",
                "week_range",
            ]
        )

    # Create pivot table for qset-grade-wise representation of median time-taken for grade jump
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
        grd_wise_overall_median_time["previous_grade_time"] = (
            grd_wise_overall_median_time["previous_grade_time"].astype(str)
            + " ("
            + grd_wise_overall_median_time["learner_id"].astype(str)
            + ")"
        )

        grd_wise_overall_median_time.drop(columns=["learner_id"], inplace=True)
    else:
        grd_wise_overall_median_time["previous_grade_time"] = np.nan

    # sort qset-grades based on their priority order
    grades_ls = grades.sort_values(by="id")["grade"]
    grd_wise_overall_median_time = grd_wise_overall_median_time.reindex(
        grades_ls, fill_value=pd.NA
    )

    grd_wise_overall_median_time = grd_wise_overall_median_time[
        grd_wise_overall_median_time.index != "class-six"
    ]

    # Create df for median time taken for grade-jump for a learner both on overall basis and grade-basis
    overall_median_grade_jump_time = pd.DataFrame(
        {
            "overall_count": [
                f"{round(final_overall_grad_jump_dt['previous_grade_time'].median(), 2)} ({ unique_learners if (unique_learners:=final_overall_grad_jump_dt['learner_id'].nunique()) else np.nan})"
            ]
            + grd_wise_overall_median_time["previous_grade_time"].to_list()
        }
    )

    overall_median_grade_jump_time.replace("nan (nan)", "", inplace=True)

    # Filters data for default if date range not selected
    if (not final_overall_grad_jump_dt.empty) and not (from_date and to_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        from_date = most_recent_monday - timedelta(days=7 * 5)
        final_overall_grad_jump_dt = final_overall_grad_jump_dt[
            (final_overall_grad_jump_dt["min_timestamp"].dt.date >= from_date.date())
        ]

    if not final_overall_grad_jump_dt.empty:
        final_overall_grad_jump_dt.loc[:, "week_range"] = final_overall_grad_jump_dt[
            "min_timestamp"
        ].dt.date.apply(lambda x: calculate_range(x))

        # Create pivot table for weekly representation of median time-taken for grade jump
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

        weekly_grd_jump_median_time = (
            weekly_median_time + " (" + weekly_unique_learners + ")"
        )
        weekly_grd_jump_median_time.reset_index(names=["metrics"], inplace=True)

        weekly_grd_jump_median_time.loc[0, "metrics"] = (
            "median time (in min) for a grade jump"
        )
    else:
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

    # Create pivot table for grade-wise weekly representation of median time-taken for grade jump
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
        grd_wise_weekly_median_time["previous_grade_time"] = (
            grd_wise_weekly_median_time["previous_grade_time"].astype(str)
            + " ("
            + grd_wise_weekly_median_time["learner_id"].astype(str)
            + ")"
        )

        grd_wise_weekly_median_time.drop(columns=["learner_id"], inplace=True)

    grd_wise_weekly_median_time = grd_wise_weekly_median_time.droplevel(0, axis=1)

    # Reindex the df based on sorted qset-grades
    grd_wise_weekly_median_time = grd_wise_weekly_median_time.reindex(
        grades_ls, fill_value=pd.NA
    )
    grd_wise_weekly_median_time = grd_wise_weekly_median_time.reset_index().rename(
        columns={"grade": "sub_metrics"}
    )

    grd_wise_weekly_median_time = grd_wise_weekly_median_time[
        grd_wise_weekly_median_time["sub_metrics"] != "class-six"
    ]

    grade_jump_map = {
        "class-one": "class-one to class-two",
        "class-two": "class-two to class-three",
        "class-three": "class-three to class-four",
        "class-four": "class-four to class-five",
        "class-five": "class-five to class-six",
    }

    grd_wise_weekly_median_time["sub_metrics"] = grd_wise_weekly_median_time[
        "sub_metrics"
    ].map(grade_jump_map)

    # Concat to create grade-jump data
    weekly_grade_jump_median_time = pd.concat(
        [weekly_grd_jump_median_time, grd_wise_weekly_median_time]
    )

    weekly_grade_jump_median_time.replace("nan (nan)", "", inplace=True)

    return overall_median_grade_jump_time, weekly_grade_jump_median_time


def get_operator_jump_data():
    operator_jump_data = all_learners_data.copy()

    if not operator_jump_data.empty:
        operator_jump_data.loc[:, "date"] = operator_jump_data["updated_at"].dt.date

        # Aggregating data
        operator_jump_data = (
            operator_jump_data.groupby(["learner_id", "grade", "operation", "date"])
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


all_operator_jump_data = get_operator_jump_data()


def get_median_time_for_operation_jump(from_date, to_date, grade):
    operator_jump_df = all_operator_jump_data.copy()

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
                & (overall_operator_jump_df["min_timestamp"].dt.date >= to_date)
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


def get_learners_metrics_data(from_date: str, to_date: str, grade: str, operation: str):
    # Fetch learners proficiency data
    all_learners_data_df = all_learners_data.copy()

    if not (from_date and to_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        past_5_weeks_date = most_recent_monday - timedelta(days=7 * 5)
        from_date = past_5_weeks_date.date()
    else:
        from_date = pd.to_datetime(from_date).date()
        to_date = pd.to_datetime(to_date).date()

    previous_learners_list = all_learners_data_df[
        all_learners_data_df["updated_at"].dt.date < from_date
    ]["learner_id"].unique()

    if not all_learners_data_df.empty:
        # Create updated_date column from updated_at
        all_learners_data_df["updated_date"] = all_learners_data_df[
            "updated_at"
        ].dt.date

    """ UNIQUE LEARNERS LOGIC"""
    overall_unique_learners, weekly_uni_lrs_cnt_table, final_unique_learners_df = (
        get_unique_learners(all_learners_data_df, from_date, to_date, grade, operation)
    )

    """ NEW LEARNERS ADDED LOGIC """
    weekly_new_learners_added = get_new_learners_added(
        final_unique_learners_df, previous_learners_list
    )

    """ SESSIONS COUNT LOGIC"""
    overall_sessions, weekly_sessions_cnt_table = get_sessions(
        all_learners_data_df, from_date, to_date, grade
    )

    """ WORK DONE LOGIC"""
    overall_work_done, weekly_work_done, work_done_df = get_work_done(
        all_learners_data_df, from_date, to_date, grade, operation
    )

    """ WORK DONE PER LEARNER LOGIC"""
    overall_work_done_avg, weekly_work_done_per_lr = get_avg_work_done_per_learner(
        work_done_df, overall_work_done, overall_unique_learners
    )

    """ MEDIAN WORK DONE PER LEARNER LOGIC"""
    overall_median_work_done, weekly_median_work_done = (
        get_median_work_done_per_learner(work_done_df)
    )

    """ TOTAL TIME TAKEN LOGIC """
    overall_time_taken, weekly_total_time, total_time_df = get_total_time_taken(
        all_learners_data_df, from_date, to_date, grade, operation
    )

    """ AVERAGE TIME PER LEARNER LOGIC"""
    overall_time_taken_avg, weekly_time_taken_per_lr = avg_time_taken_per_learner(
        total_time_df, overall_time_taken, overall_unique_learners
    )

    """MEDIAN ACCURACY OF LEARNERS LOGIC"""

    overall_median_accuracy, weekly_median_accuracy = get_median_accuracy(
        all_learners_data_df, from_date, to_date, grade, operation
    )

    """ MEDIAN TIME TAKEN FOR GRADE JUMP """

    overall_median_grade_jump_time, weekly_grade_jump_median_time = (
        get_median_time_for_grade_jump(from_date, to_date, grade, operation)
    )

    """ MEDIAN TIME TAKEN FOR OPERATOR JUMP"""
    overall_median_operator_jump_time, weekly_operator_jump_median_time = (
        get_median_time_for_operation_jump(from_date, to_date, grade)
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


# Callback to update the table based on selected learner_id
@callback(
    Output("dig-lpm-learner-perf-data-table", "data"),
    Output("dig-lpm-learner-perf-data-table", "columns"),
    Output("dig-lpm-learner-perf-data-table", "style_data_conditional"),
    Input("dig-lpm-dates-picker", "start_date"),
    Input("dig-lpm-dates-picker", "end_date"),
    Input("dig-lpm-grades-dropdown", "value"),
    Input("dig-lpm-operations-dropdown", "value"),
)
def update_table(start_date, end_date, selected_grade, selected_operation):
    # Filter the DataFrame based on the selected schools, school_grades and learner_id
    filtered_data = get_learners_metrics_data(
        start_date, end_date, selected_grade, selected_operation
    )

    if not (start_date and end_date):
        current_date = datetime.today()
        most_recent_monday = current_date - timedelta(days=current_date.weekday())
        start_date = (most_recent_monday - timedelta(days=7 * 5)).date()
        end_date = current_date.date()
    else:
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

    week_columns = generate_week_ranges(start_date, end_date)
    non_week_columns = [
        {"name": col.replace("_", " ").title(), "id": col}
        for col in ["metrics", "sub_metrics", "overall_count"]
    ]
    columns = non_week_columns + week_columns

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

    return (
        filtered_data.to_dict("records"),
        columns,
        style_data_conditional,
    )  # Return the filtered data


# callback for button which clears the selected date
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


# Function to check the format of columns as week-range column
def check_column_format(input_column):
    # Define the regular expression pattern for the format 'YYYY-MM-DD,YYYY-MM-DD %'
    pattern = r"\d{4}-\d{2}-\d{2},\d{4}-\d{2}-\d{2}"
    # Check if the column matches the pattern
    if re.search(pattern, input_column):
        return True
    return False


@callback(
    Output("dig-learners-list-data-table", "data"),
    Output("dig-ll-div", "hidden"),
    Output("dig-ll-selected-filters", "children"),
    Input("dig-lpm-learner-perf-data-table", "active_cell"),
    Input("dig-ll-qset-purpose-dropdown", "value"),
    Input("dig-ll-grades-dropdown", "value"),
    Input("dig-ll-operations-dropdown", "value"),
    State("dig-lpm-dates-picker", "start_date"),
    State("dig-lpm-dates-picker", "end_date"),
    State("dig-lpm-grades-dropdown", "value"),
    State("dig-lpm-operations-dropdown", "value"),
)
def update_learners_list_table(
    active_cell,
    qset_types,
    selected_grade,
    selected_operation,
    from_date,
    to_date,
    parent_grade,
    parent_operation,
):
    learners_attempt_table_data = pd.DataFrame(columns=["learner_id", "grade"])
    hidden = True
    selected_filters = ""
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
        if (is_week_column or is_overall_column) and (
            operation := operation_map.get(row)
        ):
            hidden = False
            if selected_operation:
                operation = selected_operation

            learners_attempts_data = all_learners_data[
                all_learners_data["operation"] == operation
            ].copy()

            if not selected_operation and parent_operation:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["operation"] == parent_operation
                ]

            if is_week_column:
                from_date, to_date = column.split(",")
                from_date_dt = pd.to_datetime(from_date, utc=True)
                to_date_dt = pd.to_datetime(to_date, utc=True)
            else:
                from_date_dt = (
                    pd.to_datetime(from_date, utc=True) if from_date else None
                )
                to_date_dt = pd.to_datetime(to_date, utc=True) if to_date else None

            if from_date_dt:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["updated_at"] >= from_date_dt
                ]
            if to_date_dt:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["updated_at"] <= to_date_dt
                ]

            if selected_grade:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["grade"] == selected_grade
                ]
            elif parent_grade:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["grade"] == parent_grade
                ]

            if qset_types:
                learners_attempts_data = learners_attempts_data[
                    learners_attempts_data["purpose"].isin(qset_types)
                ]

            selected_time_frame = (
                f"Time Frame: {from_date} - {to_date if to_date else 'present'}"
                if from_date or to_date
                else f"Time Frame: Overall"
            )
            selected_operation = f"Operation: {operation}"
            selected_filters = (
                f"SELECTED METRICS :- {selected_operation}, {selected_time_frame}"
            )
            if not learners_attempts_data.empty:
                grouped_data = (
                    learners_attempts_data.groupby(
                        ["learner_id", "grade", "qset_grade"]
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

                learners_attempt_table_data = pd.concat(
                    [attempt_data, accuracy_data], axis=1
                ).reset_index()

                grade_mapping = grouped_data.set_index("learner_id")["grade"].to_dict()
                learners_attempt_table_data["grade"] = learners_attempt_table_data[
                    "learner_id"
                ].map(grade_mapping)

    return learners_attempt_table_data.to_dict("records"), hidden, selected_filters


@callback(
    Output("dig-learner-info-table", "data"),
    Output("dig-li-div", "hidden"),
    Output("dig-li-heading", "children"),
    Input("dig-learners-list-data-table", "active_cell"),
    Input("dig-learners-list-data-table", "data"),
    Input("dig-li-qset-grades-dropdown", "value"),
    Input("dig-li-operations-dropdown", "value"),
    Input("dig-li-qset-purpose-dropdown", "value"),
)
def update_learner_info_table(active_cell, data, qset_grade, operation, qset_types):
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
    if active_cell and data and active_cell["row"] < len(data):
        column = active_cell["column_id"]
        if column == "learner_id":
            hidden = False
            learner_id = data[active_cell["row"]]["learner_id"]
            selected_learner_data = all_learners_data[
                all_learners_data["learner_id"] == learner_id
            ].copy()

            heading = f"SELECTED LEARNER :- ID: {learner_id} , Grade: {selected_learner_data['grade'].iloc[0]}"

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

            if not selected_learner_data.empty:
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
                            lambda x: ",".join(x),
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


""" FINAL LAYOUT OF THE PAGE """

layout = html.Div(
    [
        html.H1("Master Dashboard"),
        html.P(id="placeholder"),
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
                dcc.DatePickerRange(
                    id="dig-lpm-dates-picker",
                    min_date_allowed=min_max_date.loc[0, "min"].date(),
                    max_date_allowed=min_max_date.loc[0, "max"].date(),
                ),
                # Dropdown for selecting grade
                dcc.Dropdown(
                    id="dig-lpm-grades-dropdown",
                    options=[
                        {"label": school_grade, "value": school_grade}
                        for school_grade in grades.sort_values(by="id")[
                            "grade"
                        ].unique()
                    ],
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
                dcc.Loading(
                    id="dig-ll-loading-table",
                    type="circle",
                    children=[
                        html.H4(
                            id="dig-ll-selected-filters", style={"margin": "10px 10px"}
                        ),
                        html.Div(
                            [
                                # Dropdown for selecting grade
                                dcc.Dropdown(
                                    id="dig-ll-grades-dropdown",
                                    options=[
                                        {"label": school_grade, "value": school_grade}
                                        for school_grade in grades.sort_values(by="id")[
                                            "grade"
                                        ].unique()
                                    ],
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
                                    options=[
                                        {"label": qset_type, "value": qset_type}
                                        for qset_type in qset_types["purpose"]
                                        .sort_values()
                                        .unique()
                                    ],
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
                        dash_table.DataTable(
                            id="dig-learners-list-data-table",
                            columns=[
                                {"name": ["", "Learner ID"], "id": "learner_id"},
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
                                # Dropdown for selecting grade
                                dcc.Dropdown(
                                    id="dig-li-qset-grades-dropdown",
                                    options=[
                                        {"label": school_grade, "value": school_grade}
                                        for school_grade in grades.sort_values(by="id")[
                                            "grade"
                                        ].unique()
                                    ],
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
                                    options=[
                                        {"label": qset_type, "value": qset_type}
                                        for qset_type in qset_types["purpose"]
                                        .sort_values()
                                        .unique()
                                    ],
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

# if __name__ == "__main__":
#     app.run_server(debug=True)
