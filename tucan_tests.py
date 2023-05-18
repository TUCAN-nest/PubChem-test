import pipeline
import sqlite3
from typing import Callable
from functools import partial
from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text
from tucan.canonicalization import canonicalize_molecule
from tucan.serialization import serialize_molecule


def test_invariance_consumer(molfile: str, get_molfile_id: Callable):
    assertion = "passed"
    try:
        permutation_invariance(graph_from_molfile_text(molfile))
    except AssertionError as exception:
        assertion = str(exception)

    return (
        "regression",
        pipeline.get_current_time(),
        get_molfile_id(molfile),
        assertion,
    )


def test_regression_consumer(molfile: str, get_molfile_id: Callable):
    tucan_string = serialize_molecule(
        canonicalize_molecule(graph_from_molfile_text(molfile))
    )

    return (
        "regression ",
        pipeline.get_current_time(),
        get_molfile_id(molfile),
        tucan_string,
    )


def invariance_driver(sdf_path: str, log_path: str, get_molfile_id: Callable):
    with sqlite3.connect(log_path) as log_db:
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(
                test_invariance_consumer, get_molfile_id=get_molfile_id
            ),
            number_of_consumer_processes=8,
        )

        for time, molfile_id, assertion in log_db.execute(
            "SELECT time, molfile_id, result FROM results"
        ):
            if assertion != "passed":
                print(
                    f"{time}: invariance test failed for molfile {molfile_id}: {assertion}."
                )


def regression_driver(
    sdf_path: str,
    log_path: str,
    reference_path: str,
    get_molfile_id: Callable,
):
    with (
        sqlite3.connect(":memory:") as intermediate_log_db,
        sqlite3.connect(log_path) as log_db,
        sqlite3.connect(reference_path) as reference_db,
    ):
        intermediate_log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=intermediate_log_db,
            consumer_function=partial(
                test_regression_consumer, get_molfile_id=get_molfile_id
            ),
            number_of_consumer_processes=8,
        )

        intermediate_log_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

        for molfile_id, reference_result in reference_db.execute(
            "SELECT molfile_id, result FROM results"
        ):
            # TODO: guard look up in reference db
            current_result = intermediate_log_db.execute(
                "SELECT result FROM results WHERE molfile_id = ?",
                (molfile_id,),
            ).fetchone()[0]

            assertion = "passed"
            try:
                assert reference_result == current_result
            except AssertionError as exception:
                assertion = str(exception)

            log_db.execute(
                "INSERT INTO results VALUES (?, ?, ?, ?)",
                ("regression", pipeline.get_current_time(), molfile_id, assertion),
            )


def regression_reference_driver(
    sdf_path: str,
    log_path: str,
    get_molfile_id: Callable,
):
    with sqlite3.connect(log_path) as log_db:
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(
                test_regression_consumer, get_molfile_id=get_molfile_id
            ),
            number_of_consumer_processes=8,
        )
