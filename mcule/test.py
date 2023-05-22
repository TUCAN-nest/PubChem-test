import re
from argparse import ArgumentParser
from typing import Final
import tucan_consumers
from drivers import invariance_driver, regression_driver, regression_reference_driver

"""
python -m mcule.test regression --result-destination path/to/log --compute-reference-results
python -m mcule.test regression --result-destination path/to/log --regression-reference path/to/reference
python -m mcule.test invariance --result-destination path/to/log
python -m mcule.test all --result-destination path/to/log --regression-reference
"""

SDF_PATH: Final = "mcule/testdata/mcule_20000.sdf.gz"


def _get_mcule_id(molfile: str) -> str:
    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    molfile_id_match = molfile_id_pattern.search(molfile)
    molfile_id = molfile_id_match.group(1).strip() if molfile_id_match else ""
    return molfile_id


if __name__ == "__main__":
    parser = ArgumentParser(description="Run tests against mcule SDF.")
    subparsers = parser.add_subparsers(
        required=True, dest="test_type", title="test-type"
    )

    result_destination_args = {
        "default": ":memory:",
        "metavar": "RESULT_DESTINATION",
        "help": "Path to save the results. If not specified, results will be saved in memory.",
    }

    regression_reference_args = {
        "metavar": "REGRESSION_REFERENCE",
        "help": "Path to reference results. The current run will be compared against those results.",
    }

    invariance_parser = subparsers.add_parser("invariance")
    invariance_parser.add_argument("--result-destination", **result_destination_args)

    all_parser = subparsers.add_parser("all")
    all_parser.add_argument("--result-destination", **result_destination_args)
    all_parser.add_argument("--regression-reference", **regression_reference_args)

    regression_parser = subparsers.add_parser("regression")
    regression_parser.add_argument("--result-destination", **result_destination_args)
    group = regression_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--regression-reference", **regression_reference_args)
    group.add_argument(  # boolean flag, defaults to False
        "--compute-reference",
        action="store_true",
        help="Compute reference results against which subsequent runs will be compared.",
    )

    args = parser.parse_args()

    if args.test_type in ["invariance", "all"]:
        invariance_driver(
            sdf_path=SDF_PATH,
            log_path=args.result_destination,
            consumer=tucan_consumers.test_invariance,
            get_molfile_id=_get_mcule_id,
        )

    if args.test_type in ["regression", "all"]:
        if args.compute_reference:
            regression_reference_driver(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                consumer=tucan_consumers.test_regression,
                get_molfile_id=_get_mcule_id,
            )
        else:
            regression_driver(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                reference_path=args.regression_reference,
                consumer=tucan_consumers.test_regression,
                get_molfile_id=_get_mcule_id,
            )
