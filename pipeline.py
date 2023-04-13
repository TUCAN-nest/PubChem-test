import multiprocessing
import gzip
import re
from time import ctime
from typing import Final, Callable, Iterator

from tucan.test_utils import permutation_invariance
from tucan.io import graph_from_molfile_text


def test_invariance(molfile):
    assertion = "passed"
    try:
        permutation_invariance(graph_from_molfile_text(molfile))
    except AssertionError as e:
        assertion = e

    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    return {
        "molfile_id": molfile_id_pattern.search(molfile).group(1).strip(),
        "result": assertion,
    }


SDF_PATH: Final[str] = "testdata/mcule_2000.sdf.gz"
LOG_PATH: Final[str] = "testdata/mcule_2000_log.txt"
CONSUMER_FUNCTION = test_invariance
NUMBER_OF_CONSUMER_PROCESSES: Final[int] = 4  # multiprocessing.cpu_count() - 1


def read_molfiles_from_zipped_sdf(sdf_path: str) -> Iterator[str]:
    """Generator yielding molfiles from gzipped SDF file.
    (G)unzips the SDF on a line-by-line basis to avoid loading entire SDF into memory.
    TODO: harden SDF parsing according to
    http://www.dalkescientific.com/writings/diary/archive/2020/09/18/handling_the_sdf_record_delimiter.html

    https://en.wikipedia.org/wiki/Chemical_table_file#SDF
    """
    current_molfile = ""
    with gzip.open(sdf_path, "rb") as gzipped_sdf:
        for gunzipped_line in gzipped_sdf:
            line = gunzipped_line.decode("utf-8", "backslashreplace")
            if "$$$$" in line:
                yield current_molfile
                current_molfile = ""
            else:
                current_molfile += line


def produce_molfiles(
    molfile_queue: multiprocessing.Queue, sdf_path: str, n_poison_pills: int
):
    for molfile in read_molfiles_from_zipped_sdf(sdf_path):
        molfile_queue.put(molfile)

    for _ in range(n_poison_pills):
        molfile_queue.put("DONE")  # poison pill: tell consumer processes we're done

    print(f"{ctime()}: done producing molfiles")


def consume_molfiles(
    molfile_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    consumer_function: Callable,
    process_id: int,
):
    n_tested = 0
    for molfile in iter(molfile_queue.get, "DONE"):
        result_queue.put(consumer_function(molfile))
        n_tested += 1
        if not n_tested % 100:
            print(f"{ctime()}: process {process_id} consumed {n_tested} molfiles.")

    result_queue.put("DONE")

    print(f"{ctime()}: process {process_id} finished.")


if __name__ == "__main__":
    molfile_queue: multiprocessing.Queue = multiprocessing.Queue()  # TODO: limit size?
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print(f"{ctime()}: starting producer process.")
    producer_process = multiprocessing.Process(
        target=produce_molfiles,
        args=(molfile_queue, SDF_PATH, NUMBER_OF_CONSUMER_PROCESSES),
    )
    producer_process.start()

    print(
        f"{ctime()}: distributing consumer function over {NUMBER_OF_CONSUMER_PROCESSES} processes."
    )
    consumer_processes = [
        multiprocessing.Process(
            target=consume_molfiles,
            args=(molfile_queue, result_queue, CONSUMER_FUNCTION, process_id),
        )
        for process_id in range(NUMBER_OF_CONSUMER_PROCESSES)
    ]
    for consumer_process in consumer_processes:
        consumer_process.start()

    # processes won't join before all queues their
    # interacting with are empty
    with open(LOG_PATH, "w") as log:
        log.write("time\tmolfile_id\tresult\n")
        for result in iter(result_queue.get, "DONE"):
            log.write(f"{ctime()}")
            for value in result.values():
                log.write(f"\t{value}")
            log.write("\n")

    producer_process.join()
    for consumer_process in consumer_processes:
        consumer_process.join()
