import pipeline
import sqlite3
from typing import Callable
from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text
from tucan.canonicalization import canonicalize_molecule
from tucan.serialization import serialize_molecule


def test_invariance_consumer(molfile: str, get_molfile_id: Callable):
    assertion = "passed"
    try:
        permutation_invariance(graph_from_molfile_text(molfile))
    except AssertionError as e:
        assertion = e

    return (
        "regression",
        pipeline.get_current_time(),
        get_molfile_id(molfile),
        assertion,
    )


def invariance_driver(sdf_path: str, get_molfile_id: Callable, log_path: str):
    with sqlite3.connect(log_path) as log_db:
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=test_invariance_consumer,
            get_molfile_id=get_molfile_id,
            number_of_consumer_processes=8,
        )

        for time, molfile_id, assertion in log_db.execute(
            "SELECT time, molfile_id, result FROM results"
        ):
            if assertion != "passed":
                print(
                    f"{time}: invariance test failed for molfile {molfile_id}: {assertion}."
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


def regression_driver(
    sdf_path: str,
    get_molfile_id: Callable,
    log_path: str,
    reference_path: str = "",
):
    with sqlite3.connect(log_path) as log_db:
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=test_regression_consumer,
            get_molfile_id=get_molfile_id,
            number_of_consumer_processes=8,
        )

        log_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

        with sqlite3.connect(reference_path) as reference_db:
            for molfile_id, reference_tucan in reference_db.execute(
                "SELECT molfile_id, result FROM results"
            ):
                current_run_tucan = log_db.execute(
                    "SELECT result FROM results WHERE molfile_id = ?",
                    (molfile_id,),
                ).fetchone()[0]

                assert reference_tucan == current_run_tucan


def regression_reference_driver(
    sdf_path: str,
    get_molfile_id: Callable,
    log_path: str,
):
    with sqlite3.connect(log_path) as log_db:
        log_db.execute(
            "CREATE TABLE IF NOT EXISTS results (test, time, molfile_id, result)"
        )

        pipeline.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=test_regression_consumer,
            get_molfile_id=get_molfile_id,
            number_of_consumer_processes=8,
        )
