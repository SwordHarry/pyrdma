from src.common.common import check_wc_status


def process_wc_send_events(cmid, poll_count=1):
    npolled = 0
    wcs = []
    while npolled < poll_count:
        wc = cmid.get_send_comp()
        if wc is not None:
            npolled += 1
            wcs.append(wc)
    for wc in wcs:
        check_wc_status(wc)


def process_wc_recv_events(cmid, poll_count=1):
    npolled = 0
    wcs = []
    while npolled < poll_count:
        wc = cmid.get_recv_comp()
        if wc is not None:
            npolled += 1
            wcs.append(wc)
    for wc in wcs:
        check_wc_status(wc)
