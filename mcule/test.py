import re
from typing import Final
import tucan_consumers
from sdf_pipeline import drivers

SDF_PATH: Final = "mcule/testdata/mcule_2000.sdf.gz"


def _get_mcule_id(molfile: str) -> str:
    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    molfile_id_match = molfile_id_pattern.search(molfile)
    molfile_id = molfile_id_match.group(1).strip() if molfile_id_match else ""

    return molfile_id


if __name__ == "__main__":
    args = drivers.parse_cli_args()

    if args.test_type == "invariance":
        drivers.invariance(
            sdf_path=SDF_PATH,
            log_path=args.result_destination,
            consumer_function=tucan_consumers.test_invariance,
            get_molfile_id=_get_mcule_id,
        )

    if args.test_type == "regression":
        if args.compute_reference_result:
            drivers.regression_reference(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                consumer_function=tucan_consumers.test_regression,
                get_molfile_id=_get_mcule_id,
            )
        else:
            drivers.regression(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                reference_path=args.reference_result,
                consumer_function=tucan_consumers.test_regression,
                get_molfile_id=_get_mcule_id,
            )
