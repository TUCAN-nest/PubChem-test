"""https://en.wikipedia.org/wiki/Chemical_table_file#SDF"""

import gzip
from typing import Iterator


def read_molfiles_from_gzipped_sdf(gzipped_sdf_path: str) -> Iterator[str]:
    """Generator yielding molfiles from gzipped SDF file.
    (G)unzips the SDF on a line-by-line basis to avoid loading entire SDF into memory.
    """

    current_molfile = ""
    with gzip.open(gzipped_sdf_path, "rb") as gzipped_sdf:
        for gunzipped_line in gzipped_sdf:
            line = gunzipped_line.decode("utf-8")
            if line == "$$$$\n":
                yield current_molfile
                current_molfile = ""
            else:
                current_molfile += line
