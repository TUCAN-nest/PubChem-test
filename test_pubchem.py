from ftp import download_all_sdf_from_pubchem
from sdf import read_molfiles_from_gzipped_sdf
import multiprocessing
from time import ctime
from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text
from typing import Final


def consume_sdf(sdf_queue, result_queue, pid):
    n_tested = 0
    for sdf_filepath in iter(sdf_queue.get, "DONE"):
        for molfile in read_molfiles_from_gzipped_sdf(sdf_filepath):
            try:
                permutation_invariance(graph_from_molfile_text(molfile))
            except AssertionError as e:
                result_queue.put(e)
            n_tested += 1
            if not n_tested % 1000:
                print(f"{ctime()}: process {pid} tested {n_tested} molecules.")


if __name__ == "__main__":
    NUMBER_OF_PROCESSES: Final[int] = 4  # multiprocessing.cpu_count() - 1

    sdf_queue: multiprocessing.Queue = (
        multiprocessing.Queue()
    )  # queue will at most be as long as number of SDF files on PubChem
    result_queue = multiprocessing.Queue()

    print(f"{ctime()}: distributing tests of {NUMBER_OF_PROCESSES} processes.")
    processes = [
        multiprocessing.Process(target=consume_sdf, args=(sdf_queue, result_queue, pid))
        for pid in range(NUMBER_OF_PROCESSES)
    ]
    for p in processes:
        p.start()

    for filepath in download_all_sdf_from_pubchem():
        sdf_queue.put(filepath)

    for _ in range(NUMBER_OF_PROCESSES):
        sdf_queue.put("DONE")  # tell consumer processes we're done

    for p in processes:
        p.join()  # wait for process to finish
        p.close()

    while not result_queue.empty():
        print(result_queue.get())
