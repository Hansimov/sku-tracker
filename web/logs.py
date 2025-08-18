from tclogger import logger, logstr


def log_link_idx(link_idx: int, total_links: int):
    logger.note(
        f"[{logstr.mesg(link_idx + 1)}/{logstr.file(total_links)}]",
        end=" ",
    )
