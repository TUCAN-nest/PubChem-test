import tucan_consumers
from pubchem.ftp import download_all_sdf
from sdf_pipeline import drivers


def _get_pubchem_id(molfile):
    return molfile.split()[0].strip()


if __name__ == "__main__":
    args = drivers.parse_cli_args()

    for sdf_path in download_all_sdf(destination_directory="testdata"):
        drivers.invariance(
            sdf_path=sdf_path,
            log_path=args.result_destination,
            consumer_function=tucan_consumers.test_invariance,
            get_molfile_id=_get_pubchem_id,
        )
