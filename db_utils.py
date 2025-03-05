import gzip
import pickle
import time
from datetime import datetime, timedelta

import pandas as pd
import redis
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

import config

# Create the connection string for the database
connection_string = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"

# Create a SQLAlchemy engine with connection pooling
engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,  # Number of connections to keep in the pool
    max_overflow=10,  # Additional connections allowed beyond pool_size
    pool_timeout=30,  # Timeout for acquiring a connection from the pool
)

# Create a session factory
Session = sessionmaker(bind=engine)

# Redis client
redis_client = redis.Redis(
    host=config.REDIS_HOST,
    port=int(config.REDIS_PORT),
    username=config.REDIS_USER,
    password=config.REDIS_PASSWORD,
)

# Key names
ALL_LEARNER_DATA_KEY = "learners_data_json"
LAST_FETCHED_TIME_KEY = "last_fetched_time"
MAX_TIME_KEY = "max_time"
MIN_TIME_KEY = "min_time"
LAST_QUESTION_PER_QSET_GRADE_KEY = "last_question_per_qset_grade"
ALL_LEARNERS_KEY = "all_learners"
ALL_GRADES_KEY = "all_grades"
ALL_SCHOOLS_KEY = "all_schools"
ALL_QSET_TYPES_KEY = "all_qset_types"
ALL_REPOSITORY_NAMES_KEY = "all_repository_names"
ALL_SKILLS_KEY = "all_skills"
ALL_TENANTS_KEY = "all_tenants"
ALL_LOGGED_IN_USERS_KEY = "all_logged_in_users"
ALL_QUESTION_SEQUENCE_DATA = "all_question_sequence_data"


def execute_query_with_retry(query, max_retries=3, delay=1, dtype=None):
    conn = engine.connect().execution_options(stream_results=True)

    for attempt in range(max_retries):
        try:
            final_df = pd.DataFrame()
            for chunk_dataframe in pd.read_sql(query, conn, chunksize=10000):
                print(f"Processing chunk with {len(chunk_dataframe)} rows")
                print(
                    f"Memory usage: {chunk_dataframe.memory_usage(index=True, deep=True).sum() / 1024 ** 2:.2f} MB"
                )
                final_df = pd.concat([final_df, chunk_dataframe], ignore_index=True)
            return final_df.astype(dtype=dtype)
        except exc.OperationalError as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)


def get_learners_data(last_updated_at=None):
    query = f"""
    SELECT
    tn.name->>'en' AS tenant_name,
    sc.name AS school,
    cm.name->>'en' AS grade,
    lr.name AS learner_name,
    lr.username AS learner_username,
    lpd.learner_id,
    lpd.question_id,
    lpd.question_set_id,
    lpd.updated_at,
    lpd.attempts_count,
    lpd.score,
    lpd.taxonomy->'class'->>'identifier' AS qset_grade_identifier,
    lpd.taxonomy->'l1_skill'->>'identifier' AS operation_identifier,
    qs.title->>'en' AS qset_name,
    qs.x_id AS qset_uid,
    qs.purpose,
    qs.repository->>'identifier' AS repo_name_identifier,
    qs.taxonomy->'l1_skill'->>'identifier' AS l1_skill_identifier,
    qs.taxonomy->'l2_skill'->0->>'identifier' AS l2_skill_identifier,
    qs.taxonomy->'l3_skill'->0->>'identifier' AS l3_skill_identifier,
    qs.sequence AS sequence,
    lj.status
    FROM learner_proficiency_question_level_data lpd
    LEFT JOIN question_set qs ON lpd.question_set_id = qs.identifier
    LEFT JOIN learner lr ON lpd.learner_id = lr.identifier
    LEFT JOIN class_master cm ON lr.class_id = cm.identifier
    LEFT JOIN school sc ON lr.school_id = sc.identifier
    LEFT JOIN learner_journey lj ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN tenant tn ON lr.tenant_id = tn.identifier
    """

    dtype_dict = {
        "tenant_name": "category",
        "school": "category",
        "grade": "category",
        "learner_name": "category",
        "learner_username": "category",
        "learner_id": "string",  # Use string for UUIDs
        "question_id": "string",
        "question_set_id": "string",
        "attempts_count": "int8",
        "score": "int8",
        "qset_grade_identifier": "string",
        "operation_identifier": "string",
        "qset_name": "category",
        "qset_uid": "string",
        "purpose": "category",
        "repo_name_identifier": "string",
        "l1_skill_identifier": "string",
        "l2_skill_identifier": "string",
        "l3_skill_identifier": "string",
        "sequence": "int16",
        "status": "category",
    }

    if last_updated_at:
        query = query + f" WHERE lpd.updated_at >= '{last_updated_at.isoformat()}'"
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_last_question_per_qset_grade():
    query = """
    WITH ranked_question_sets AS (
        SELECT
            identifier,
            taxonomy->'l1_skill'->'name'->>'en' AS operation,
            taxonomy->'class'->'name'->>'en' AS qset_grade,
            sequence,
            ROW_NUMBER() OVER (PARTITION BY taxonomy->'class'->'name'->>'en', taxonomy->'l1_skill'->'name'->>'en' ORDER BY sequence DESC) AS rn
        FROM question_set
    ),
    ranked_questions AS (
        SELECT
            question_id,
            question_set_id,
            sequence,
            ROW_NUMBER() OVER (PARTITION BY question_set_id ORDER BY sequence DESC) AS rn
        FROM question_set_question_mapping
    )
    SELECT
        rqs.operation,
        rqs.qset_grade,
        rq.question_set_id AS question_set_id,
        rq.question_id AS question_id
    FROM ranked_question_sets rqs    
    LEFT JOIN ranked_questions rq
    ON rqs.identifier = rq.question_set_id
    WHERE rqs.rn = 1 AND rq.rn=1;
    """

    dtype_dict = {
        "operation": "category",
        "qset_grade": "category",
        "question_set_id": "string",
        "question_id": "string",
    }

    return execute_query_with_retry(query, dtype=dtype_dict)


def get_all_learners():
    query = """
        SELECT 
        DISTINCT lr.identifier, 
        lr.username as user_name, 
        lr.name as name, 
        sc.name as school 
        FROM learner lr LEFT JOIN school sc 
        ON lr.school_id = sc.identifier
    """

    dtype_dict = {
        "identifier": "string",
        "user_name": "category",
        "name": "category",
        "school": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


grades_priority = {
    1: "class-one",
    2: "class-two",
    3: "class-three",
    4: "class-four",
    5: "class-five",
    6: "class-six",
    7: "class-seven",
    8: "class-eight",
    9: "class-nine",
    10: "class-ten",
}


def get_grades():
    query = "SELECT identifier, id, cm.name->>'en' AS grade FROM class_master cm"
    dtype_dict = {
        "grade": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_schools():
    query = "SELECT name as school_name FROM school"
    dtype_dict = {
        "school_name": "category",
    }
    schools = execute_query_with_retry(query, dtype=dtype_dict)
    schools = pd.concat(
        [schools, pd.DataFrame({"school_name": ["No School"]})], ignore_index=True
    )
    schools["school_name"] = schools["school_name"].astype("category")
    return schools


def get_qset_types():
    query = "SELECT DISTINCT(purpose) FROM question_set"
    dtype_dict = {
        "purpose": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_repository_names():
    query = "SELECT identifier, name->>'en' AS repo_name FROM repository"
    dtype_dict = {
        "repo_name": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_skills():
    query = "SELECT identifier, name->>'en' as skill, type FROM skill_master"
    dtype_dict = {
        "skill": "category",
        "type": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_tenants():
    query = "SELECT DISTINCT(id), name->>'en' AS tenant_name FROM tenant"
    dtype_dict = {
        "tenant_name": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_logged_in_users():
    query = """
        SELECT td.id, td.level, td.learner_id, td.created_on, sc.name as school, cm.name->>'en' as grade, tn.name->>'en' as tenant_name
        FROM telemetry_data td
        LEFT JOIN learner lr ON lr.identifier = td.learner_id
        LEFT JOIN school sc ON sc.identifier = lr.school_id
        LEFT JOIN class_master cm ON cm.identifier = lr.class_id
        LEFT JOIN tenant tn ON tn.identifier = lr.tenant_id
        WHERE td.event_type = 'learner_logged_in'
    """
    dtype_dict = {
        "school": "category",
        "grade": "category",
        "tenant_name": "category",
    }
    return execute_query_with_retry(query, dtype=dtype_dict)


def get_question_sequence_data():
    print("Fetching question_sequence_data")
    query = f"""
    SELECT question_id, question_set_id, sequence FROM question_set_question_mapping
    """
    dtype_dict = {
        "question_id": "string",
        "question_set_id": "string",
        "sequence": "int16",
    }
    question_sequence_data = execute_query_with_retry(query, dtype=dtype_dict)
    return question_sequence_data


def get_cached_data(key):
    last_fetched_time = redis_client.get(LAST_FETCHED_TIME_KEY)
    cached_data = redis_client.get(key)

    if (not cached_data) or (not last_fetched_time):
        fetch_all_data()
    elif last_fetched_time:
        last_fetched_time = datetime.fromisoformat(last_fetched_time.decode("utf-8"))
        # If last fetched time is less than 1 hour, return cached data
        if (datetime.now() - last_fetched_time) > timedelta(hours=1):
            fetch_all_data()

    # Return the data for the requested key
    if key == ALL_LEARNER_DATA_KEY:
        return pickle.loads(gzip.decompress(redis_client.get(key)))
    elif key in [LAST_FETCHED_TIME_KEY, MAX_TIME_KEY, MIN_TIME_KEY]:
        return redis_client.get(key)
    return pickle.loads(redis_client.get(key))


def store_in_redis(key, data):
    """Serialize and store data in Redis."""
    redis_client.set(key, data)


def map_and_merge(df, ref_data, left_key, right_key, new_column):
    """Generalized function to map and merge reference data."""
    ref_data = ref_data.rename(columns={"skill": new_column})
    df = df.merge(
        ref_data[[right_key, new_column]],
        left_on=left_key,
        right_on=right_key,
        how="left",
    ).drop(columns=[right_key, left_key])
    return df


def update_cache():
    """Fetch and update static datasets in Redis."""
    cache_data = {
        LAST_QUESTION_PER_QSET_GRADE_KEY: get_last_question_per_qset_grade(),
        ALL_LEARNERS_KEY: get_all_learners(),
        ALL_GRADES_KEY: get_grades(),
        ALL_SCHOOLS_KEY: get_schools(),
        ALL_QSET_TYPES_KEY: get_qset_types(),
        ALL_REPOSITORY_NAMES_KEY: get_repository_names(),
        ALL_SKILLS_KEY: get_skills(),
        ALL_TENANTS_KEY: get_tenants(),
        ALL_LOGGED_IN_USERS_KEY: get_logged_in_users(),
        ALL_QUESTION_SEQUENCE_DATA: get_question_sequence_data(),
    }

    for key, data in cache_data.items():
        store_in_redis(key, pickle.dumps(data))


def process_learners_data(updated_data):
    """Process and map learners' data with reference tables."""
    grades = pickle.loads(redis_client.get(ALL_GRADES_KEY))
    grades["grade"] = grades["id"].map(grades_priority)

    # Map qset_grade
    updated_data = updated_data.merge(
        grades[["identifier", "grade"]].rename(columns={"grade": "qset_grade"}),
        left_on="qset_grade_identifier",
        right_on="identifier",
        how="left",
    ).drop(columns=["identifier", "qset_grade_identifier"])

    # Map grade name
    updated_data["grade"] = updated_data["grade"].astype(int).map(grades_priority)

    # Fetch all skills
    skills = pickle.loads(redis_client.get(ALL_SKILLS_KEY))

    # Handle L1 Skill mapping (including 'operation')
    l1_skill = skills[skills["type"] == "l1_skill"].rename(
        columns={"skill": "l1_skill"}
    )
    updated_data = updated_data.merge(
        l1_skill[["identifier", "l1_skill"]].rename(columns={"l1_skill": "operation"}),
        left_on="operation_identifier",
        right_on="identifier",
        how="left",
    ).drop(columns=["identifier", "operation_identifier"])

    # Generalized mapping for skill levels (L1, L2, L3)
    skill_types = ["l1_skill", "l2_skill", "l3_skill"]
    for skill_type in skill_types:
        filtered_skills = skills[skills["type"] == skill_type]
        updated_data = map_and_merge(
            updated_data,
            filtered_skills,
            f"{skill_type}_identifier",
            "identifier",
            skill_type,
        )

    # Map repository names
    repo = pickle.loads(redis_client.get(ALL_REPOSITORY_NAMES_KEY))
    updated_data = map_and_merge(
        updated_data, repo, "repo_name_identifier", "identifier", "repo_name"
    )

    # Fill missing school values
    updated_data["school"] = (
        updated_data["school"].cat.add_categories("No School").fillna("No School")
    )

    return updated_data


def fetch_all_data():
    update_cache()

    if redis_client.get(ALL_LEARNER_DATA_KEY):
        learner_data = pickle.loads(
            gzip.decompress(redis_client.get(ALL_LEARNER_DATA_KEY))
        )
        max_updated_at = pd.to_datetime(learner_data["updated_at"]).max()
        updated_data = get_learners_data(max_updated_at)

        if not updated_data.empty:
            updated_data = process_learners_data(updated_data)
            all_learners_data = (
                pd.concat([learner_data, updated_data])
                .drop_duplicates()
                .reset_index(drop=True)
            )

            store_in_redis(
                ALL_LEARNER_DATA_KEY, gzip.compress(pickle.dumps(all_learners_data))
            )
            redis_client.set(LAST_FETCHED_TIME_KEY, datetime.now().isoformat())
            redis_client.set(
                MAX_TIME_KEY, all_learners_data["updated_at"].max().isoformat()
            )

            print(
                f"Updated cache with {updated_data.shape[0]} new records for {ALL_LEARNER_DATA_KEY}"
            )
        else:
            redis_client.set(LAST_FETCHED_TIME_KEY, datetime.now().isoformat())
            print(f"No new updates for {ALL_LEARNER_DATA_KEY}. Returning cached data.")
    else:
        all_learners_data = get_learners_data()
        all_learners_data = process_learners_data(all_learners_data)

        store_in_redis(
            ALL_LEARNER_DATA_KEY, gzip.compress(pickle.dumps(all_learners_data))
        )
        redis_client.set(LAST_FETCHED_TIME_KEY, datetime.now().isoformat())
        redis_client.set(
            MIN_TIME_KEY, all_learners_data["updated_at"].min().isoformat()
        )
        redis_client.set(
            MAX_TIME_KEY, all_learners_data["updated_at"].max().isoformat()
        )

    print("Updated cache with new data for all keys")


def get_data(key):
    return get_cached_data(key)


def get_all_learners_data_df():
    return get_data(ALL_LEARNER_DATA_KEY)


def get_repository_names_list():
    repo = get_data(ALL_REPOSITORY_NAMES_KEY)
    return repo["repo_name"].sort_values().unique()


def get_l1_skills_list():
    skills = get_data(ALL_SKILLS_KEY)
    l1_skill = skills[skills["type"] == "l1_skill"]
    l1_skill = l1_skill.rename(columns={"skill": "l1_skill"})
    return l1_skill["l1_skill"].sort_values().unique()


def get_l2_skills_list():
    skills = get_data(ALL_SKILLS_KEY)
    l2_skill = skills[skills["type"] == "l2_skill"]
    l2_skill = l2_skill.rename(columns={"skill": "l2_skill"})
    return l2_skill["l2_skill"].sort_values().unique()


def get_l3_skills_list():
    skills = get_data(ALL_SKILLS_KEY)
    l3_skill = skills[skills["type"] == "l3_skill"]
    l3_skill = l3_skill.rename(columns={"skill": "l3_skill"})
    return l3_skill["l3_skill"].sort_values().unique()


def get_schools_list():
    schools = get_data(ALL_SCHOOLS_KEY)
    print(schools)
    return schools["school_name"].sort_values().unique()


def get_qset_types_list():
    qset_types = get_data(ALL_QSET_TYPES_KEY)
    return qset_types["purpose"].sort_values().unique()


def get_tenants_list():
    tenants = get_data(ALL_TENANTS_KEY)
    return tenants["tenant_name"].sort_values().unique()


def get_grades_list():
    grades = get_data(ALL_GRADES_KEY)
    grades["grade"] = grades["id"].map(grades_priority)
    return grades.drop(columns=["identifier"])


# Minimum - Maximum Learners Data Timestamp
def get_min_max_timestamp(key):
    print("Fetching get_min_max_timestamp")
    # Minimum - Maximum Date
    min_timestamp = (
        get_data(MIN_TIME_KEY).decode("utf-8")
        if get_data(MIN_TIME_KEY)
        else datetime.now()
    )
    max_timestamp = (
        get_data(MAX_TIME_KEY).decode("utf-8")
        if get_data(MAX_TIME_KEY)
        else datetime.now()
    )

    if key == "min":
        return min_timestamp
    return max_timestamp


def last_synced_time():
    print("Fetching last_synced_time")
    last_synced_at = get_data(LAST_FETCHED_TIME_KEY)

    return last_synced_at.decode("utf-8")


def get_non_diagnostic_data():
    print("Fetching non_diagnostic_data")
    all_learners_data = get_all_learners_data_df()
    non_diagnostic_data = all_learners_data[
        all_learners_data["purpose"] != "Main Diagnostic"
    ]
    return non_diagnostic_data


def get_all_learners_df():
    return get_data(ALL_LEARNERS_KEY)


def get_logged_in_users_data_df():
    return get_data(ALL_LOGGED_IN_USERS_KEY)


def get_question_sequence_data_df():
    return get_data(ALL_QUESTION_SEQUENCE_DATA)


def get_last_question_per_qset_grade_df():
    return get_data(LAST_QUESTION_PER_QSET_GRADE_KEY)


def get_all_question_sets(repository_name):
    print("Fetching all_question_sets")
    # Fetch distinct question set IDs from the database
    query = f"""
        SELECT DISTINCT(qs.x_id) AS qset_id FROM question_set qs LEFT JOIN repository repo ON repo.identifier = qs.repository->>'identifier'
    """

    # Add condition for repository name if provided
    if repository_name:
        query += f" WHERE repo.name->>'en'='{repository_name}'"

    dtype_dict = {
        "qset_id": "string",
    }
    question_set_ids = execute_query_with_retry(query, dtype=dtype_dict)
    return question_set_ids["qset_id"].sort_values().unique()


def get_question_level_data(selected_qset):
    print("Fetching question_level_data")
    query = f"""
        SELECT
            lpd.question_set_id,
            qs.x_id AS question_set_uid,
            qs.sequence AS qs_seq,
            lpd.question_id,
            ques.x_id AS question_uid,
            qsqm.sequence AS q_seq,
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
    dtype_dict = {
        "question_set_id": "string",
        "question_set_uid": "string",
        "qs_seq": "int16",
        "question_id": "string",
        "question_uid": "string",
        "q_seq": "int16",
        "learner_id": "string",
        "score": "int8",
    }

    question_level_data = execute_query_with_retry(query, dtype=dtype_dict)
    return question_level_data


def get_qset_score_data(selected_sheet_type, selected_repo):
    print("Fetching qset_score_data")
    # Filter query based on selected question set type
    sheet_type_filter = ""
    if selected_sheet_type:
        sheet_type_filter = f"AND qs.purpose = '{selected_sheet_type}'"

    # Filter query based on selected repository
    repo_filter = ""
    if selected_repo:
        repo_filter = f"AND repo.name->>'en' = '{selected_repo}'"

    # Query to fetch detailed question set level data
    query = f"""
    SELECT lpd.taxonomy->'l1_skill'->'name'->'en' AS operation,
        lpd.taxonomy->'class'->'name'->'en' AS qset_grade,
        lpd.question_set_id,
        qs.title->'en' AS qset_name,
        avg(lpd.score) AS avg_score
    FROM learner_journey lj
    LEFT JOIN learner_proficiency_question_set_level_data lpd ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN question_set qs ON qs.identifier = lj.question_set_id
    LEFT JOIN repository repo ON repo.identifier = qs.repository->>'identifier'
    WHERE lj.status='completed' {sheet_type_filter} {repo_filter}
    GROUP BY lpd.taxonomy->'l1_skill'->'name'->'en', lpd.taxonomy->'class'->'name'->'en', lpd.question_set_id, qs.title->'en'
    """
    dtype_dict = {
        "operation": "category",
        "qset_grade": "category",
        "question_set_id": "string",
        "qset_name": "category",
    }
    qset_score_data = execute_query_with_retry(query, dtype=dtype_dict)
    return qset_score_data


def get_qset_agg_data(selected_sheet_type, selected_repo):
    print("Fetching qset_agg_data")
    # Filter query based on selected question set type
    sheet_type_filter = ""
    if selected_sheet_type:
        sheet_type_filter = f"AND qs.purpose = '{selected_sheet_type}'"

    # Filter query based on selected repository
    repo_filter = ""
    if selected_repo:
        repo_filter = f"AND repo.name->>'en' = '{selected_repo}'"

    # Query to fetch aggregated question set level data
    query = f"""
    SELECT lpd.taxonomy->'l1_skill'->'name'->'en' AS operation,
        lpd.taxonomy->'class'->'name'->'en' AS qset_grade,
        COUNT(*) AS attempts_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lpd.score) AS median,
        avg(lpd.score)
    FROM learner_journey lj
    LEFT JOIN learner_proficiency_question_set_level_data lpd ON lj.question_set_id = lpd.question_set_id AND lj.learner_id = lpd.learner_id
    LEFT JOIN question_set qs ON qs.identifier = lj.question_set_id
    LEFT JOIN repository repo ON repo.identifier = qs.repository->>'identifier'
    WHERE lj.status='completed' {sheet_type_filter} {repo_filter}
    GROUP BY lpd.taxonomy->'l1_skill'->'name'->'en', lpd.taxonomy->'class'->'name'->'en'
    """
    dtype_dict = {
        "operation": "category",
        "qset_grade": "category",
        "attempts_count": "int8",
    }
    qset_agg_data = execute_query_with_retry(query, dtype=dtype_dict)
    return qset_agg_data
