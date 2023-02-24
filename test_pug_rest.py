import requests
from tucan.io.molfile_reader import graph_from_molfile_text

# from tucan.visualization import print_molecule
from tucan.test_utils import permutation_invariance
import cProfile
import pstats
from pstats import SortKey

# https://pubchem.ncbi.nlm.nih.gov//docs/pug-rest

# All PubChem web pages (or requests to NCBI in general) have a policy that users should throttle their web page requests,
# which includes web-based programmatic services.
# Violation of usage policies may result in the user being temporarily blocked from accessing PubChem (or NCBI) resources.
# The current request volume limits are:

# - No more than 5 requests per second.
# - No more than 400 requests per minute.
# - No longer than 300 second running time per minute.

# Retrieve multiple compounds with one request.

# 30s is the default timeout on PubChem servers

# https://realpython.com/python-concurrency/


def graph_from_pubchem_cid(cid: int, session: requests.Session):
    print(cid)

    response = session.get(
        f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/sdf", timeout=10
    )

    return graph_from_molfile_text(response.text)


with requests.Session() as session:
    for cid in range(1, 101):
        m = graph_from_pubchem_cid(cid, session)
        permutation_invariance(m)


# with cProfile.Profile() as pr:
#     with requests.Session() as session:
#         for cid in range(1, 6):
#             m = graph_from_pubchem_cid(cid, session)
#             permutation_invariance(m)

#     pr.dump_stats("profile")


# p = pstats.Stats("profile")
# p.sort_stats(SortKey.CUMULATIVE).print_stats(100)
