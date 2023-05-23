from typing import Callable
from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text
from tucan.canonicalization import canonicalize_molecule
from tucan.serialization import serialize_molecule
from sdf_pipeline import utils


def test_invariance(molfile: str, get_molfile_id: Callable) -> utils.ConsumerResult:
    assertion = "passed"
    try:
        permutation_invariance(graph_from_molfile_text(molfile))
    except AssertionError as exception:
        assertion = str(exception)

    return utils.ConsumerResult(
        "invariance",
        utils.get_current_time(),
        get_molfile_id(molfile),
        assertion,
    )


def test_regression(molfile: str, get_molfile_id: Callable) -> utils.ConsumerResult:
    tucan_string = serialize_molecule(
        canonicalize_molecule(graph_from_molfile_text(molfile))
    )

    return utils.ConsumerResult(
        "regression ",
        utils.get_current_time(),
        get_molfile_id(molfile),
        tucan_string,
    )
