from config import DATA_LAKE, WHITEHOUSE_LOGS as WHL

"""
    catalog of all resources
"""

def _resource(zone, project, key):
    """
     takes storage params t=and return path for resource in lake

    :param zone: zone of data lake
    :param project: project for while resource is being added
    :param key: key to actual resource
    :return: path of resource in string format
    """
    return str(DATA_LAKE / zone / project / key)


catalog = {
    "landing/whitehouse_logs": _resource("landing", WHL, "*.csv"),

    "clean/whitehouse_logs_combined": _resource("clean", WHL, "whl_logs_raw_combined.csv"),
    "clean/whitehouse_logs_cleaned": _resource("clean", WHL, "whl_logs_raw_cleaned.csv"),
    "clean/whitehouse_logs_processed": _resource("clean", WHL, "whl_logs_raw_processed.csv"),

    "business/frequent_visitors": _resource("business", WHL, "10_most_frequent_visitors.csv"),
    "business/frequent_visitees": _resource("business", WHL, "10_most_frequent_visitees.csv"),
    "business/frequent_pairs": _resource("business", WHL, "10_most_frequent_pairs.csv"),

}