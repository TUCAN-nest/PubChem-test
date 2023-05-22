# TUCAN tests

:warning: **This repository is not used for "official" testing yet. Prototyping only.** :warning:

## Notes
[Tucan invariance tests](https://github.com/TUCAN-nest/TUCAN/blob/187a0d40c7ffca1855f7ad78f7a190d0e73e9b2c/tucan/test_utils.py#L9-L20) against 1 [PubChem SDF](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/) (containing 500,000 compounds),
distributed over 8 processes ran for about 42 minutes (2502 seconds).
Assuming that PubChem contains about 150,000,000 compounds,
the expected runtime is `150,000,000 compounds / 500,000 compounds * 42 minutes = 12600 minutes = 210 hours = 8.75 days`.
The runtime could be reduced significantly by distributing the test over more processes.