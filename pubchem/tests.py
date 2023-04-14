import re
import pipeline
from pubchem.ftp import download_all_sdf_from_pubchem
from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text


def test_invariance(molfile):
    assertion = "passed"
    try:
        permutation_invariance(graph_from_molfile_text(molfile))
    except AssertionError as e:
        assertion = e

    molfile_id = molfile.split()[0].strip()

    return {
        "molfile_id": molfile_id,
        "arguments": "",
        "result": assertion,
    }


if __name__ == "__main__":
    for sdf_path in download_all_sdf_from_pubchem("testdata"):
        pipeline.run(
            sdf_path=sdf_path,
            log_path=f"{sdf_path}.log",
            consumer_functions=[test_invariance],
            number_of_consumer_processes=8,
        )
